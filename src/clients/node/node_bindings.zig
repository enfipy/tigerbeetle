const std = @import("std");
const vsr = @import("vsr");

const assert = std.debug.assert;
const tb = vsr.tigerbeetle;
const tb_client = vsr.tb_client;

const TypeMapping = struct {
    name: []const u8,
    hidden_fields: []const []const u8 = &.{},
    docs_link: ?[]const u8 = null,

    pub fn hidden(comptime self: @This(), name: []const u8) bool {
        inline for (self.hidden_fields) |field| {
            if (std.mem.eql(u8, field, name)) {
                return true;
            }
        } else return false;
    }
};

const type_mappings = .{
    .{ tb.AccountFlags, TypeMapping{
        .name = "AccountFlags",
        .hidden_fields = &.{"padding"},
        .docs_link = "reference/account#flags",
    } },
    .{ tb.TransferFlags, TypeMapping{
        .name = "TransferFlags",
        .hidden_fields = &.{"padding"},
        .docs_link = "reference/transfer#flags",
    } },
    .{ tb.AccountFilterFlags, TypeMapping{
        .name = "AccountFilterFlags",
        .hidden_fields = &.{"padding"},
        .docs_link = "reference/account-filter#flags",
    } },
    .{ tb.QueryFilterFlags, TypeMapping{
        .name = "QueryFilterFlags",
        .hidden_fields = &.{"padding"},
        .docs_link = "reference/query-filter#flags",
    } },
    .{ tb.Account, TypeMapping{
        .name = "Account",
        .docs_link = "reference/account/#",
    } },
    .{ tb.Transfer, TypeMapping{
        .name = "Transfer",
        .docs_link = "reference/transfer/#",
    } },
    .{ tb.CreateAccountStatus, TypeMapping{
        .name = "CreateAccountStatus",
        .docs_link = "reference/requests/create_accounts#",
    } },
    .{ tb.CreateTransferStatus, TypeMapping{
        .name = "CreateTransferStatus",
        .docs_link = "reference/requests/create_transfers#",
    } },
    .{ tb.CreateAccountResult, TypeMapping{
        .name = "CreateAccountResult",
        .hidden_fields = &.{"reserved"},
    } },
    .{ tb.CreateTransferResult, TypeMapping{
        .name = "CreateTransferResult",
        .hidden_fields = &.{"reserved"},
    } },
    .{ tb.AccountFilter, TypeMapping{
        .name = "AccountFilter",
        .hidden_fields = &.{"reserved"},
        .docs_link = "reference/account-filter#",
    } },
    .{ tb.QueryFilter, TypeMapping{
        .name = "QueryFilter",
        .hidden_fields = &.{"reserved"},
        .docs_link = "reference/query-filter#",
    } },
    .{ tb.AccountBalance, TypeMapping{
        .name = "AccountBalance",
        .hidden_fields = &.{"reserved"},
        .docs_link = "reference/account-balances#",
    } },
    .{ tb_client.Operation, TypeMapping{
        .name = "Operation",
        .hidden_fields = &.{ "reserved", "root", "register" },
    } },
};

fn typescript_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .@"enum" => return comptime get_mapped_type_name(Type) orelse @compileError(
            "Type " ++ @typeName(Type) ++ " not mapped.",
        ),
        .@"struct" => |info| switch (info.layout) {
            .@"packed" => return comptime typescript_type(
                @Int(.unsigned, @bitSizeOf(Type)),
            ),
            else => return comptime get_mapped_type_name(Type) orelse @compileError(
                "Type " ++ @typeName(Type) ++ " not mapped.",
            ),
        },
        .int => |info| {
            assert(info.signedness == .unsigned);
            return switch (info.bits) {
                16 => "number",
                32 => "number",
                64 => "bigint",
                128 => "bigint",
                else => @compileError("invalid int type: " ++ @typeName(Type)),
            };
        },
        else => @compileError("Unhandled type: " ++ @typeName(Type)),
    }
}

fn get_mapped_type_name(comptime Type: type) ?[]const u8 {
    inline for (type_mappings) |type_mapping| {
        if (Type == type_mapping[0]) {
            return type_mapping[1].name;
        }
    } else return null;
}

fn emit_enum(
    buffer: *std.array_list.Managed(u8),
    comptime Type: type,
    comptime mapping: TypeMapping,
) !void {
    try emit_docs(buffer, mapping, 0, null);

    try buffer.print("export enum {s} {{\n", .{mapping.name});

    inline for (@typeInfo(Type).@"enum".field_names) |field_name| {
        if (comptime std.mem.startsWith(u8, field_name, "deprecated_")) continue;
        if (comptime mapping.hidden(field_name)) continue;

        try emit_docs(buffer, mapping, 1, field_name);

        const int_value = @intFromEnum(@field(Type, field_name));
        try buffer.print("  {s} = {s},\n", .{
            field_name,
            if (int_value == std.math.maxInt(@TypeOf(int_value)))
                std.fmt.comptimePrint("0x{X}", .{int_value})
            else
                std.fmt.comptimePrint("{}", .{int_value}),
        });
    }

    try buffer.print("}}\n\n", .{});
}

fn emit_packed_struct(
    buffer: *std.array_list.Managed(u8),
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
) !void {
    assert(type_info.layout == .@"packed");
    try emit_docs(buffer, mapping, 0, null);

    try buffer.print(
        \\export enum {s} {{
        \\  none = 0,
        \\
    , .{mapping.name});

    inline for (type_info.field_names, 0..) |field_name, i| {
        if (comptime mapping.hidden(field_name)) continue;

        try emit_docs(buffer, mapping, 1, field_name);

        try buffer.print("  {s} = (1 << {d}),\n", .{
            field_name,
            i,
        });
    }

    try buffer.print("}}\n\n", .{});
}

fn emit_struct(
    buffer: *std.array_list.Managed(u8),
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
) !void {
    try emit_docs(buffer, mapping, 0, null);

    try buffer.print("export type {s} = {{\n", .{
        mapping.name,
    });

    inline for (type_info.field_names, type_info.field_types) |field_name, field_type| {
        if (comptime mapping.hidden(field_name)) continue;

        try emit_docs(buffer, mapping, 1, field_name);

        switch (@typeInfo(field_type)) {
            .array => try buffer.print("  {s}: Buffer\n", .{
                field_name,
            }),
            else => try buffer.print(
                "  {s}: {s}\n",
                .{
                    field_name,
                    typescript_type(field_type),
                },
            ),
        }
    }

    try buffer.print("}}\n\n", .{});
}

fn emit_docs(
    buffer: anytype,
    comptime mapping: TypeMapping,
    comptime indent: comptime_int,
    comptime field: ?[]const u8,
) !void {
    if (mapping.docs_link) |docs_link| {
        try buffer.print(
            \\
            \\{[indent]s}/**
            \\{[indent]s} * See [{[name]s}](https://docs.tigerbeetle.com/{[docs_link]s}{[field]s})
            \\{[indent]s} */
            \\
        , .{
            .indent = vsr.stdx.comptime_repeat("  ", indent),
            .name = field orelse mapping.name,
            .docs_link = docs_link,
            .field = field orelse "",
        });
    }
}

pub fn generate_bindings(buffer: *std.array_list.Managed(u8)) !void {
    @setEvalBranchQuota(100_000);

    try buffer.print(
        \\///////////////////////////////////////////////////////
        \\// This file was auto-generated by node_bindings.zig //
        \\//              Do not manually modify.              //
        \\///////////////////////////////////////////////////////
        \\
        \\
    , .{});

    // Emit JS declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const mapping = type_mapping[1];

        switch (@typeInfo(ZigType)) {
            .@"struct" => |info| switch (info.layout) {
                .auto => @compileError(
                    "Only packed or extern structs are supported: " ++ @typeName(ZigType),
                ),
                .@"packed" => try emit_packed_struct(buffer, info, mapping),
                .@"extern" => try emit_struct(buffer, info, mapping),
            },
            .@"enum" => try emit_enum(buffer, ZigType, mapping),
            else => @compileError("Type cannot be represented: " ++ @typeName(ZigType)),
        }
    }
}

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = std.array_list.Managed(u8).init(allocator);
    try generate_bindings(&buffer);
    var stdout_buffer: [std.heap.page_size_min]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    try stdout_writer.interface.writeAll(buffer.items);
    try stdout_writer.interface.flush();
}
