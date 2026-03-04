const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const log = std.log;

pub const std_options: std.Options = .{
    .log_level = .info,
};

pub fn main(process: std.process.Init) !void {
    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const arena = arena_instance.allocator();
    const cwd = std.Io.Dir.cwd();

    var process_threaded = std.Io.Threaded.init(arena, .{
        .environ = if (std.Options.debug_threaded_io) |threaded|
            threaded.environ.process_environ
        else
            std.process.Environ.empty,
    });
    defer process_threaded.deinit();

    const process_io = process_threaded.ioBasic();

    var args_it = try std.process.Args.Iterator.initAllocator(process.minimal.args, arena);
    defer args_it.deinit();

    var args: [7][]const u8 = undefined;
    var args_len: usize = 0;
    while (args_it.next()) |arg| {
        assert(args_len < args.len);
        args[args_len] = arg;
        args_len += 1;
    }
    assert(args_len == 6 or args_len == 7);

    _, const zig, const global_cache, const url, const file_name, const out = args[0..6].*;
    const hash_optional = if (args_len == 7) args[6] else null;

    if (hash_optional) |hash| {
        // Fast path --- don't touch the Internet if we have the hash locally.
        const cached = path_join(arena, &.{ global_cache, "p", hash, file_name });
        if (std.Io.Dir.copyFile(cwd, cached, cwd, out, std.Options.debug_io, .{})) {
            log.debug("download skipped: cache hit", .{});
            return;
        } else |_| { // Time to ask for forgiveness!
            log.debug("download: cache miss", .{});
        }
    } else {
        log.debug("download: no hash", .{});
    }

    const hash = try fetch(arena, .{
        .process_io = process_io,
        .zig = zig,
        .tmp = path_join(arena, &.{ global_cache, "tmp" }),
        .url = url,
    });

    if (hash_optional) |hash_specified| {
        if (!std.mem.eql(u8, hash, hash_specified)) {
            log.err(
                \\bad hash
                \\specified: {s}
                \\fetched:   {s}
                \\
            , .{ hash_specified, hash });
            return error.BadHash;
        }
    }

    const cached = path_join(arena, &.{ global_cache, "p", hash, file_name });
    errdefer log.err("copying from {s}", .{cached});

    try std.Io.Dir.copyFile(cwd, cached, cwd, out, std.Options.debug_io, .{});
}

/// If curl is available, use it for robust downloads, and then
/// `zig fetch` a local file to get the hash. Otherwise, fetch
/// the url directly.
fn fetch(arena: Allocator, options: struct {
    process_io: std.Io,
    zig: []const u8,
    tmp: []const u8,
    url: []const u8,
}) ![]const u8 {
    if (exec_ok(arena, options.process_io, &.{ "curl", "--version" })) {
        log.debug("download: curl", .{});
        const url_file_name = options.url[std.mem.lastIndexOf(u8, options.url, "/").?..];
        var random_bytes: [8]u8 = undefined;
        std.Options.debug_io.random(&random_bytes);
        const tmp_dir = path_join(arena, &.{
            options.tmp,
            &std.fmt.bytesToHex(random_bytes, .lower),
        });
        defer std.Io.Dir.deleteTree(std.Io.Dir.cwd(), std.Options.debug_io, tmp_dir) catch {};

        try std.Io.Dir.createDirPath(std.Io.Dir.cwd(), std.Options.debug_io, tmp_dir);

        const curl_output = path_join(arena, &.{ tmp_dir, url_file_name });
        _ = try exec(arena, options.process_io, &(.{
            "curl",             "--retry-all-errors",
            "--retry",          "5",
            "--retry-max-time", "120",
            "--retry-delay",    "30",
            "--location",       options.url,
            "--output",         curl_output,
        }));
        return try exec(arena, options.process_io, &.{ options.zig, "fetch", curl_output });
    }
    log.debug("download: zig fetch", .{});
    return try exec(arena, options.process_io, &.{ options.zig, "fetch", options.url });
}

fn path_join(arena: Allocator, components: []const []const u8) []const u8 {
    return std.fs.path.join(arena, components) catch |err| oom(err);
}

fn exec_ok(arena: Allocator, process_io: std.Io, argv: []const []const u8) bool {
    assert(argv.len > 0);
    const result = std.process.run(arena, process_io, .{ .argv = argv }) catch return false;
    return switch (result.term) {
        .exited => |code| code == 0,
        else => false,
    };
}

fn exec(arena: Allocator, process_io: std.Io, argv: []const []const u8) ![]const u8 {
    assert(argv.len > 0);
    const result = std.process.run(arena, process_io, .{ .argv = argv }) catch |err| {
        log.err("running {s}: {}", .{ argv[0], err });
        return err;
    };
    if (switch (result.term) {
        .exited => |code| code != 0,
        else => true,
    }) {
        log.err("running {s}: {}\n{s}", .{ argv[0], result.term, result.stderr });
        return error.Exec;
    }
    if (std.mem.indexOfScalar(u8, result.stdout, '\n')) |first_newline| {
        if (first_newline + 1 == result.stdout.len) {
            return result.stdout[0 .. result.stdout.len - 1];
        }
    }
    return result.stdout;
}

fn oom(_: error{OutOfMemory}) noreturn {
    @panic("OOM");
}
