const std = @import("std");
const stdx = @import("stdx");
const vsr = @import("vsr");
const constants = vsr.constants;
const Operation = vsr.tigerbeetle.Operation;

// We could have used the idiomatic Zig API exposed by `vsr.tb_client`,
// but we want to test the actual FFI exposed by `libtb_client`.
const c = @cImport({
    @cInclude("tb_client.h");
});

const assert = std.debug.assert;
const Mutex = std.Io.Mutex;
const Condition = std.Io.Condition;

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
    @"cluster-id": u128,
    addresses: []const u8,
};

pub fn main(init: std.process.Init) !void {
    var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    defer switch (gpa_allocator.deinit()) {
        .ok => {},
        .leak => @panic("memory leak"),
    };

    const allocator = gpa_allocator.allocator();
    var args_iterator = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args_iterator.deinit();

    const args = stdx.flags(&args_iterator, CLIArgs);

    const cluster_id = args.@"cluster-id";
    log.info("addresses: {s}", .{args.addresses});

    var tb_client: c.tb_client_t = undefined;
    const init_status = c.tb_client_init(
        &tb_client,
        std.mem.asBytes(&cluster_id),
        args.addresses.ptr,
        @intCast(args.addresses.len),
        0,
        on_complete,
    );
    if (init_status != c.TB_INIT_SUCCESS) {
        return error.ClientInitError;
    }
    defer {
        const client_status = c.tb_client_deinit(&tb_client);
        assert(client_status == c.TB_CLIENT_OK);
    }

    var stdin_buffer: [4096]u8 = undefined;
    var stdout_buffer: [4096]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().reader(std.Options.debug_io, &stdin_buffer);
    var stdout_writer = std.Io.File.stdout().writer(std.Options.debug_io, &stdout_buffer);

    while (true) {
        var events_buffer: [events_buffer_size_max]u8 = undefined;
        const operation, const events = receive(
            &stdin_reader.interface,
            events_buffer[0..],
        ) catch |err| {
            switch (err) {
                error.EndOfStream => break,
                else => return err,
            }
        };

        var context = RequestContext{};

        {
            context.lock.lockUncancelable(std.Options.debug_io);
            defer context.lock.unlock(std.Options.debug_io);

            var packet: c.tb_packet_t = undefined;
            packet.operation = @intFromEnum(operation);
            packet.user_data = @ptrCast(@constCast(&context));
            packet.data = @constCast(events.ptr);
            packet.data_size = @intCast(events.len);
            packet.user_tag = 0;
            packet.status = c.TB_PACKET_OK;

            const client_status = c.tb_client_submit(&tb_client, &packet);
            assert(client_status == c.TB_CLIENT_OK);

            while (!context.completed) {
                context.condition.waitUncancelable(std.Options.debug_io, &context.lock);
            }
        }

        try write_results(
            &stdout_writer.interface,
            operation,
            context.result[0..context.result_size],
        );
        try stdout_writer.interface.flush();
    }
}

const RequestContext = struct {
    lock: Mutex = .init,
    condition: Condition = .init,
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

    context.lock.lockUncancelable(std.Options.debug_io);
    defer context.lock.unlock(std.Options.debug_io);

    assert(tb_packet.*.status == c.TB_PACKET_OK);
    assert(result != null);

    stdx.copy_disjoint(.exact, u8, context.result[0..result_size], result.?[0..result_size]);
    context.result_size = result_size;
    context.completed = true;
    context.condition.signal(std.Options.debug_io);
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
    const operation = try reader.takeEnum(Operation, .little);
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
