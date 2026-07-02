const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const stdb = @import("./stdb.zig");
const builtin = @import("builtin");

const log = std.log;

pub const std_options: std.Options = .{
    .log_level = .info,
};

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const io = init.io;

    const args = try init.minimal.args.toSlice(arena);
    assert(args.len == 6 or args.len == 7);

    _, const zig, const global_cache, const url, const file_name, const out = args[0..6].*;
    const hash_optional = if (args.len == 7) args[6] else null;
    assert(args.len <= 7);

    if (hash_optional) |hash| {
        // Fast path --- don't touch the Internet if we have the hash locally.
        const cached = path_join(arena, &.{ global_cache, "p", hash, file_name });
        if (std.Io.Dir.cwd().copyFile(cached, std.Io.Dir.cwd(), out, io, .{})) {
            log.debug("download skipped: cache hit", .{});
            return;
        } else |_| { // Time to ask for forgiveness!
            log.debug("download: cache miss", .{});
        }
    } else {
        log.debug("download: no hash", .{});
    }

    const hash = try fetch(arena, io, .{
        .zig = zig,
        .global_cache = global_cache,
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

    if (std.Io.Dir.cwd().copyFile(cached, std.Io.Dir.cwd(), out, io, .{})) {
        try set_executable(io, out);
        return;
    } else |_| {
        const archive = try std.fmt.allocPrint(arena, "{s}/p/{s}.tar.gz", .{ global_cache, hash });
        const archive_member = path_join(arena, &.{ hash, file_name });
        const contents = try stdb.exec(arena, io, &.{
            "tar",
            "-xzf",
            archive,
            "-O",
            archive_member,
        });
        try std.Io.Dir.cwd().writeFile(io, .{
            .sub_path = out,
            .data = contents,
            .flags = .{ .exclusive = false },
        });
        try set_executable(io, out);
    }
}

fn set_executable(io: std.Io, path: []const u8) !void {
    if (@import("builtin").os.tag == .windows) return;

    const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .write_only });
    defer file.close(io);

    try file.setPermissions(io, .fromMode(0o755));
}

/// If curl is available, use it for robust downloads, and then
/// `zig fetch` a local file to get the hash. Otherwise, fetch
/// the url directly.
fn fetch(arena: Allocator, io: std.Io, options: struct {
    zig: []const u8,
    global_cache: []const u8,
    tmp: []const u8,
    url: []const u8,
}) ![]const u8 {
    if (stdb.exec_ok(arena, io, &.{ "curl", "--version" })) {
        log.debug("download: curl", .{});
        const url_file_name = options.url[std.mem.lastIndexOf(u8, options.url, "/").?..];
        const tmp_dir = path_join(arena, &.{
            options.tmp,
            &std.fmt.bytesToHex(std.mem.asBytes(&unique_u128()), .lower),
        });
        defer std.Io.Dir.cwd().deleteTree(io, tmp_dir) catch {};

        try std.Io.Dir.cwd().createDirPath(io, tmp_dir);

        const curl_output = path_join(arena, &.{ tmp_dir, url_file_name });
        // TODO Go back to using stdb.exec once this curl/zip issue is debugged.
        const curl_result = std.process.run(arena, io, .{
            .argv = &(.{
                "curl",             "--retry-all-errors",
                "--retry",          "5",
                "--retry-max-time", "120",
                "--retry-delay",    "30",
                "--location",       options.url,
                "--output",         curl_output,
                "--verbose",        "--fail",
            }),
            .stdout_limit = .limited(1024 * 1024),
            .stderr_limit = .limited(1024 * 1024),
        }) catch |err| {
            log.err("curl error: {}", .{err});
            return err;
        };
        errdefer log.err("curl stderr: {s}\n\ncurl stderr end", .{curl_result.stderr});

        if (!(curl_result.term == .exited and curl_result.term.exited == 0)) {
            log.err("curl error: {}", .{curl_result.term});
            return error.Exec;
        }
        return try stdb.exec(arena, io, &.{
            options.zig,
            "fetch",
            "--global-cache-dir",
            options.global_cache,
            curl_output,
        });
    }
    log.debug("download: zig fetch", .{});
    return try stdb.exec(arena, io, &.{
        options.zig,
        "fetch",
        "--global-cache-dir",
        options.global_cache,
        options.url,
    });
}

fn path_join(arena: Allocator, components: []const []const u8) []const u8 {
    return std.fs.path.join(arena, components) catch |err| oom(err);
}

pub fn oom(_: error{OutOfMemory}) noreturn {
    @panic("OOM");
}

fn unique_u128() u128 {
    var value: u128 = undefined;
    switch (builtin.os.tag) {
        .linux => {
            const buffer = std.mem.asBytes(&value);
            var index: usize = 0;
            while (index < buffer.len) {
                const slice = buffer[index..];
                const rc = std.os.linux.getrandom(slice.ptr, slice.len, 0);
                switch (std.posix.errno(rc)) {
                    .SUCCESS => index += @intCast(rc),
                    .INTR => continue,
                    else => |err| std.debug.panic("getrandom failed: {}", .{err}),
                }
            }
        },
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => {
            const buffer = std.mem.asBytes(&value);
            std.c.arc4random_buf(buffer.ptr, buffer.len);
        },
        else => @compileError("fetch needs secure randomness wiring for this OS"),
    }
    assert(value != 0);
    return value;
}
