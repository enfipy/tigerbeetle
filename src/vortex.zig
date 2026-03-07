/// This is the main entrypoint for the Vortex testing tools, delegating to these programs:
///
/// * _supervisor_: runs a cluster of multiple TigerBeetle replicas, drivers, and a workload, along
/// with various fault injection, to test the system as a whole.
/// * _driver_: a separate process communicating over stdio, using `tb_client` to send commands
/// and queries to the cluster. Drivers in other languages should be implemented elsewhere.
/// * _workload_: a separate process that, given a driver, runs commands and queries against the
/// cluster, verifying its correctness.
///
/// For practical use, Vortex should be run in a Linux namespace where it can control the network.
/// The `run` command sets up a Linux namespace automatically.
const std = @import("std");
const stdx = @import("stdx");
const builtin = @import("builtin");

const Supervisor = @import("testing/vortex/supervisor.zig");
const Workload = @import("testing/vortex/workload.zig");

const assert = std.debug.assert;

const log = std.log.scoped(.vortex);

pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = stdx.log_with_timestamp,
};

pub const CLIArgs = union(enum) {
    supervisor: Supervisor.CLIArgs,
    workload: WorkloadArgs,
};

const WorkloadArgs = struct {
    cluster: u128,
    addresses: []const u8,
    driver_command: []const u8,
};

pub fn main(process: std.process.Init) !void {
    comptime assert(builtin.target.cpu.arch.endian() == .little);

    if (builtin.os.tag == .windows) {
        log.err("vortex is not supported for Windows", .{});
        return error.NotSupported;
    }

    var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    defer switch (gpa_allocator.deinit()) {
        .ok => {},
        .leak => @panic("memory leak"),
    };

    const allocator = gpa_allocator.allocator();

    var args = try std.process.Args.Iterator.initAllocator(process.minimal.args, allocator);
    defer args.deinit();

    switch (stdx.flags(&args, CLIArgs)) {
        .supervisor => |supervisor_args| try Supervisor.main(allocator, supervisor_args),
        .workload => |driver_args| {
            var driver = try start_driver(allocator, driver_args);
            defer {
                if (driver.id != null) driver.kill(std.Options.debug_io);
            }

            try Workload.main(allocator, &.{
                .input = driver.stdin.?,
                .output = driver.stdout.?,
            });
        },
    }
}

fn start_driver(allocator: std.mem.Allocator, args: WorkloadArgs) !std.process.Child {
    var argv = std.array_list.Managed([]const u8).init(allocator);
    defer argv.deinit();

    assert(std.mem.indexOfScalar(u8, args.driver_command, '"') == null);
    var cmd_parts = std.mem.splitScalar(u8, args.driver_command, ' ');

    while (cmd_parts.next()) |part| {
        try argv.append(part);
    }

    var cluster_argument: [32]u8 = undefined;
    const cluster = try std.fmt.bufPrint(cluster_argument[0..], "{d}", .{args.cluster});

    try argv.append(cluster);
    try argv.append(args.addresses);

    return try std.process.spawn(std.Options.debug_io, .{
        .argv = argv.items,
        .stdin = .pipe,
        .stdout = .pipe,
        .stderr = .inherit,
    });
}
