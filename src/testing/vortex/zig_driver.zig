const std = @import("std");
const stdx = @import("stdx");
const vsr = @import("vsr");
const constants = vsr.constants;
const Operation = vsr.tigerbeetle.Operation;

// We could have used the idiomatic Zig API exposed by `vsr.tb_client`,
// but we want to test the actual FFI exposed by `libtb_client`.
const c = vsr.tb_client.exports;

extern fn tb_client_init(
    tb_client_out: *c.tb_client_t,
    cluster_id_ptr: *const [16]u8,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    completion_ctx: usize,
    completion_callback: c.tb_completion_t,
) callconv(.c) c.tb_init_status;
extern fn tb_client_submit(
    tb_client: ?*c.tb_client_t,
    packet: *c.tb_packet_t,
) callconv(.c) c.tb_client_status;
extern fn tb_client_deinit(tb_client: ?*c.tb_client_t) callconv(.c) c.tb_client_status;

const assert = std.debug.assert;

const log = std.log.scoped(.zig_driver);
const events_count_max = 8189;
const events_buffer_size_max = size: {
    var event_size_max = 0;
    for (std.enums.values(Operation)) |operation| {
        event_size_max = @max(event_size_max, operation.event_size());
    }
    break :size event_size_max * events_count_max;
};

pub const CLIArgs = struct {
    @"--": void,
    cluster: u128,
    addresses: []const u8,
};

pub fn main(init: std.process.Init) !void {
    var gpa_allocator = std.heap.DebugAllocator(.{}){};
    defer switch (gpa_allocator.deinit()) {
        .ok => {},
        .leak => @panic("memory leak"),
    };

    const allocator = gpa_allocator.allocator();
    var flags = stdx.Flags.init(allocator, init.minimal.args);
    defer flags.deinit(allocator);

    const args = flags.parse(CLIArgs);
    log.info("addresses: {s}", .{args.addresses});

    const addresses_z = try allocator.dupeSentinel(u8, args.addresses, 0);
    defer allocator.free(addresses_z);

    var tb_client: c.tb_client_t = undefined;
    const init_status = tb_client_init(
        &tb_client,
        std.mem.asBytes(&args.cluster),
        addresses_z.ptr,
        @intCast(args.addresses.len),
        0,
        on_complete,
    );
    if (init_status != .success) {
        return error.ClientInitError;
    }
    defer {
        const client_status = tb_client_deinit(&tb_client);
        assert(client_status == .ok);
    }

    var stdin_buffer: [4096]u8 = undefined;
    var stdin = std.Io.File.stdin().reader(init.io, &stdin_buffer);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(init.io, &stdout_buffer);

    while (true) {
        var events_buffer: [events_buffer_size_max]u8 = undefined;
        const operation, const events = receive(&stdin.interface, events_buffer[0..]) catch |err| {
            switch (err) {
                error.EndOfStream => break,
                else => return err,
            }
        };

        var context = RequestContext{ .io = init.io };

        {
            try context.lock.lock(init.io);
            defer context.lock.unlock(init.io);

            var packet: c.tb_packet_t = undefined;
            packet.operation = @intFromEnum(operation);
            packet.user_data = @ptrCast(@constCast(&context));
            packet.data = @constCast(events.ptr);
            packet.data_size = @intCast(events.len);
            packet.user_tag = 0;
            packet.status = .ok;

            const client_status = tb_client_submit(&tb_client, &packet);
            assert(client_status == .ok);

            while (!context.completed) {
                try context.condition.wait(init.io, &context.lock);
            }
        }

        write_results(
            &stdout.interface,
            operation,
            context.result[0..context.result_size],
        ) catch |err| {
            log.info("stdout is closed, exiting: {}", .{err});
            break;
        };
        stdout.interface.flush() catch |err| {
            log.info("stdout is closed, exiting: {}", .{err});
            break;
        };
    }
}

const RequestContext = struct {
    io: std.Io,
    lock: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,
    completed: bool = false,
    result: [constants.message_body_size_max]u8 = undefined,
    result_size: u32 = 0,
};

pub fn on_complete(
    tb_context: usize,
    tb_packet: [*c]c.tb_packet_t,
    timestamp: u64,
    result: ?[*]const u8,
    result_size: u32,
) callconv(.c) void {
    _ = tb_context;
    _ = timestamp;
    const context: *RequestContext = @ptrCast(@alignCast(tb_packet.*.user_data.?));

    context.lock.lockUncancelable(context.io);
    defer context.lock.unlock(context.io);

    assert(tb_packet.*.status == .ok);
    assert(result != null);

    stdx.copy_disjoint(.exact, u8, context.result[0..result_size], result.?[0..result_size]);
    context.result_size = result_size;
    context.completed = true;
    context.condition.signal(context.io);
}

fn write_results(
    writer: *std.Io.Writer,
    operation: Operation,
    result: []const u8,
) !void {
    switch (operation) {
        inline else => |operation_comptime| {
            const result_size = operation_comptime.result_size();
            if (result_size > 0) {
                const count = @divExact(result.len, result_size);
                try writer.writeInt(u32, @intCast(count), .little);
                try writer.writeAll(result);
            } else {
                log.err(
                    "unexpected size {d} for op: {s}",
                    .{ result_size, @tagName(operation_comptime) },
                );
                unreachable;
            }
        },
    }
}

fn receive(reader: *std.Io.Reader, buffer: []u8) !struct { Operation, []const u8 } {
    const operation: Operation = @enumFromInt(try reader.takeInt(u8, .little));
    const count = try reader.takeInt(u32, .little);

    return switch (operation) {
        inline else => |operation_comptime| {
            assert(count <= events_count_max);

            const response_size = operation_comptime.event_size() * count;
            assert(buffer.len >= response_size);

            try reader.readSliceAll(buffer[0..response_size]);

            return .{ operation_comptime, buffer[0..response_size] };
        },
    };
}
