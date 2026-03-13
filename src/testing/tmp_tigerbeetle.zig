//! TmpTigerBeetle is an utility for integration tests, which spawns a single node TigerBeetle
//! cluster in a temporary directory.

const std = @import("std");
const builtin = @import("builtin");

const Shell = @import("../shell.zig");

const log = std.log.scoped(.tmptigerbeetle);

const TmpTigerBeetle = @This();

/// Path to the executable.
tigerbeetle_exe: []const u8,
/// Port the TigerBeetle instance is listening on.
port: u16,
/// For convenience, the same port pre-converted to string.
port_str: []const u8,

tmp_dir: []const u8,

process: std.process.Child,

pub fn init(
    gpa: std.mem.Allocator,
    options: struct {
        development: bool,
        prebuilt: ?[]const u8 = null,
    },
) !TmpTigerBeetle {
    const shell = try Shell.create(gpa);
    defer shell.destroy();

    var from_source_path: ?[]const u8 = null;
    defer if (from_source_path) |path| gpa.free(path);

    if (options.prebuilt == null) {
        const tigerbeetle_exe = comptime "tigerbeetle" ++ builtin.target.exeFileExt();

        // If tigerbeetle binary does not exist yet, build it.
        //
        // TODO: just run `zig build run` unconditionally here, when that doesn't do spurious
        // rebuilds.
        _ = shell.project_root.statFile(std.Options.debug_io, tigerbeetle_exe, .{}) catch {
            log.info("building TigerBeetle", .{});
            try shell.exec_zig("build", .{});

            _ = try shell.project_root.statFile(std.Options.debug_io, tigerbeetle_exe, .{});
        };

        from_source_path = try shell.project_root.realPathFileAlloc(
            std.Options.debug_io,
            tigerbeetle_exe,
            gpa,
        );
    }

    const tigerbeetle_exe_input = options.prebuilt orelse from_source_path.?;
    const tigerbeetle_exe: []const u8 = if (std.fs.path.isAbsolute(tigerbeetle_exe_input)) abs: {
        break :abs try gpa.dupe(u8, tigerbeetle_exe_input);
    } else abs: {
        const absolute_z = try std.Io.Dir.realPathFileAlloc(
            std.Io.Dir.cwd(),
            std.Options.debug_io,
            tigerbeetle_exe_input,
            gpa,
        );
        defer gpa.free(absolute_z);

        break :abs try gpa.dupe(u8, absolute_z);
    };
    errdefer gpa.free(tigerbeetle_exe);

    const tmp_dir_path = try gpa.dupe(u8, try shell.create_tmp_dir());
    errdefer {
        std.Io.Dir.deleteTree(std.Io.Dir.cwd(), std.Options.debug_io, tmp_dir_path) catch {};
        gpa.free(tmp_dir_path);
    }

    const data_file: []const u8 = try std.fs.path.join(gpa, &.{ tmp_dir_path, "0_0.tigerbeetle" });
    defer gpa.free(data_file);

    try shell.exec(
        "{tigerbeetle} format --cluster=0 --replica=0 --replica-count=1 {data_file}",
        .{ .tigerbeetle = tigerbeetle_exe, .data_file = data_file },
    );

    // Pass `--addresses=0` to let the OS pick a port for us.
    var process = try shell.spawn(
        .{
            .stdin_behavior = .pipe,
            .stdout_behavior = .pipe,
            .stderr_behavior = .inherit,
        },
        "{tigerbeetle} start --development={development} --addresses=0 {data_file}",
        .{
            .tigerbeetle = tigerbeetle_exe,
            .development = if (options.development) "true" else "false",
            .data_file = data_file,
        },
    );

    errdefer process.kill(std.Options.debug_io);

    const port = port: {
        var exit_status: ?std.process.Child.Term = null;
        errdefer log.err(
            "failed to read port number from tigerbeetle process: {?}",
            .{exit_status},
        );

        var stdout_buffer: [128]u8 = undefined;
        var stdout_reader = process.stdout.?.reader(std.Options.debug_io, &stdout_buffer);
        const port_buf = stdout_reader.interface.takeDelimiterExclusive('\n') catch |err|
            switch (err) {
                error.EndOfStream => {
                    exit_status = process.wait(std.Options.debug_io) catch null;
                    return error.NoPort;
                },
                else => return err,
            };

        break :port try std.fmt.parseInt(u16, port_buf, 10);
    };

    const port_str = try std.fmt.allocPrint(gpa, "{d}", .{port});
    errdefer gpa.free(port_str);

    return TmpTigerBeetle{
        .tigerbeetle_exe = tigerbeetle_exe,
        .port = port,
        .port_str = port_str,
        .tmp_dir = tmp_dir_path,
        .process = process,
    };
}

pub fn deinit(tb: *TmpTigerBeetle, gpa: std.mem.Allocator) void {
    tb.process.kill(std.Options.debug_io);
    gpa.free(tb.port_str);
    std.Io.Dir.deleteTree(std.Io.Dir.cwd(), std.Options.debug_io, tb.tmp_dir) catch {};
    gpa.free(tb.tmp_dir);
    gpa.free(tb.tigerbeetle_exe);
}

pub fn log_stderr(tb: *TmpTigerBeetle) void {
    _ = tb;
}
