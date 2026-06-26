const std = @import("std");
const windows = std.os.windows;

pub const FALSE = windows.BOOL.FALSE;
pub const TRUE = windows.BOOL.TRUE;

pub const GENERIC_READ: windows.DWORD = 0x80000000;
pub const GENERIC_WRITE: windows.DWORD = 0x40000000;
pub const SYNCHRONIZE: windows.DWORD = 0x00100000;
pub const OPEN_EXISTING: windows.DWORD = 3;
pub const CREATE_ALWAYS: windows.DWORD = 2;
pub const FILE_ATTRIBUTE_NORMAL: windows.DWORD = 0x00000080;

pub const PIPE_ACCESS_OUTBOUND: windows.DWORD = 0x00000002;
/// FILE_FLAG_FIRST_PIPE_INSTANCE is not currently exposed by Zig's Windows bindings.
pub const FILE_FLAG_FIRST_PIPE_INSTANCE: windows.DWORD = 0x00080000;
pub const PIPE_TYPE_BYTE: windows.DWORD = 0x00000000;
pub const PIPE_WAIT: windows.DWORD = 0x00000000;
pub const INFINITE: windows.DWORD = 0xffffffff;

pub const WSA_FLAG_OVERLAPPED: windows.DWORD = 0x00000001;
pub const WSA_FLAG_NO_HANDLE_INHERIT: windows.DWORD = 0x00000080;
pub const FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: u8 = 0x1;
pub const FILE_SKIP_SET_EVENT_ON_HANDLE: u8 = 0x2;
pub const FILE_WRITE_THROUGH: windows.DWORD = 0x00000002;
pub const FILE_NO_INTERMEDIATE_BUFFERING: windows.DWORD = 0x00000008;
pub const FILE_DIRECTORY_FILE: windows.ULONG = 0x00000001;
pub const FILE_NON_DIRECTORY_FILE: windows.ULONG = 0x00000040;
pub const FILE_OPEN_REPARSE_POINT: windows.ULONG = 0x00200000;
pub const FILE_BEGIN: windows.DWORD = 0;
pub const SO_UPDATE_ACCEPT_CONTEXT: u32 = windows.ws2_32.SO.UPDATE_ACCEPT_CONTEXT;
pub const SO_UPDATE_CONNECT_CONTEXT: u32 = windows.ws2_32.SO.UPDATE_CONNECT_CONTEXT;
pub const SOL_SOCKET: c_int = windows.ws2_32.SOL.SOCKET;

pub const WinsockError = enum(u16) {
    WSA_INVALID_HANDLE = 6,
    WSA_INVALID_PARAMETER = 87,
    WSA_OPERATION_ABORTED = 995,
    WSA_IO_INCOMPLETE = 996,
    WSA_IO_PENDING = 997,
    WSAEINTR = 10004,
    WSAEBADF = 10009,
    WSAEACCES = 10013,
    WSAEFAULT = 10014,
    WSAEINVAL = 10022,
    WSAEMFILE = 10024,
    WSAEWOULDBLOCK = 10035,
    WSAEINPROGRESS = 10036,
    WSAEALREADY = 10037,
    WSAENOTSOCK = 10038,
    WSAEMSGSIZE = 10040,
    WSAEPROTOTYPE = 10041,
    WSAEOPNOTSUPP = 10045,
    WSAEAFNOSUPPORT = 10047,
    WSAEADDRINUSE = 10048,
    WSAEADDRNOTAVAIL = 10049,
    WSAENETDOWN = 10050,
    WSAENETUNREACH = 10051,
    WSAENETRESET = 10052,
    WSAECONNABORTED = 10053,
    WSAECONNRESET = 10054,
    WSAENOBUFS = 10055,
    WSAEISCONN = 10056,
    WSAENOTCONN = 10057,
    WSAESHUTDOWN = 10058,
    WSAETIMEDOUT = 10060,
    WSAECONNREFUSED = 10061,
    WSAEHOSTUNREACH = 10065,
    WSANOTINITIALISED = 10093,
    WSAEDISCON = 10101,
    _,
};

pub const WSABUF = extern struct {
    len: windows.ULONG,
    buf: [*]u8,
};

pub const OVERLAPPED = extern struct {
    Internal: usize,
    InternalHigh: usize,
    DUMMYUNIONNAME: extern union {
        DUMMYSTRUCTNAME: extern struct {
            Offset: windows.DWORD,
            OffsetHigh: windows.DWORD,
        },
        Pointer: ?*anyopaque,
    },
    hEvent: ?windows.HANDLE,
};

pub const OVERLAPPED_ENTRY = extern struct {
    lpCompletionKey: usize,
    lpOverlapped: *OVERLAPPED,
    Internal: usize,
    dwNumberOfBytesTransferred: windows.DWORD,
};

const wsadata_size = 512;

const wsa_startup_raw = @extern(
    *const fn (u16, *[wsadata_size]u8) callconv(.winapi) c_int,
    .{ .name = "WSAStartup", .library_name = "ws2_32" },
);

const wsa_cleanup_raw = @extern(
    *const fn () callconv(.winapi) c_int,
    .{ .name = "WSACleanup", .library_name = "ws2_32" },
);

const system_function_036 = @extern(
    *const fn ([*]u8, windows.ULONG) callconv(.winapi) windows.BOOLEAN,
    .{ .name = "SystemFunction036", .library_name = "advapi32" },
);

pub const get_command_line_w = @extern(
    *const fn () callconv(.winapi) windows.LPWSTR,
    .{ .name = "GetCommandLineW", .library_name = "kernel32" },
);

pub const get_environment_variable_w = @extern(
    *const fn ([*:0]const u16, [*]u16, windows.DWORD) callconv(.winapi) windows.DWORD,
    .{ .name = "GetEnvironmentVariableW", .library_name = "kernel32" },
);

pub const create_named_pipe_w = @extern(
    *const fn (
        windows.LPCWSTR,
        windows.DWORD,
        windows.DWORD,
        windows.DWORD,
        windows.DWORD,
        windows.DWORD,
        windows.DWORD,
        ?*windows.SECURITY_ATTRIBUTES,
    ) callconv(.winapi) windows.HANDLE,
    .{ .name = "CreateNamedPipeW", .library_name = "kernel32" },
);

pub const set_environment_variable_w = @extern(
    *const fn (windows.LPCWSTR, ?windows.LPCWSTR) callconv(.winapi) windows.BOOL,
    .{ .name = "SetEnvironmentVariableW", .library_name = "kernel32" },
);

pub const create_process_w = windows.kernel32.CreateProcessW;

const flush_file_buffers_raw = @extern(
    *const fn (windows.HANDLE) callconv(.winapi) windows.BOOL,
    .{ .name = "FlushFileBuffers", .library_name = "kernel32" },
);

const get_file_size_ex_raw = @extern(
    *const fn (windows.HANDLE, *i64) callconv(.winapi) windows.BOOL,
    .{ .name = "GetFileSizeEx", .library_name = "kernel32" },
);

const sleep_raw = @extern(
    *const fn (windows.DWORD) callconv(.winapi) void,
    .{ .name = "Sleep", .library_name = "kernel32" },
);

pub const duplicate_handle = @extern(
    *const fn (
        windows.HANDLE,
        windows.HANDLE,
        windows.HANDLE,
        *windows.HANDLE,
        windows.DWORD,
        windows.BOOL,
        windows.DWORD,
    ) callconv(.winapi) windows.BOOL,
    .{ .name = "DuplicateHandle", .library_name = "kernel32" },
);

pub const get_module_handle_w = @extern(
    *const fn (windows.LPCWSTR) callconv(.winapi) ?windows.HMODULE,
    .{ .name = "GetModuleHandleW", .library_name = "kernel32" },
);

pub const get_proc_address = @extern(
    *const fn (windows.HMODULE, [*:0]const u8) callconv(.winapi) ?windows.FARPROC,
    .{ .name = "GetProcAddress", .library_name = "kernel32" },
);

pub const create_file_w = @extern(
    *const fn (
        windows.LPCWSTR,
        windows.DWORD,
        windows.DWORD,
        ?*windows.SECURITY_ATTRIBUTES,
        windows.DWORD,
        windows.DWORD,
        ?windows.HANDLE,
    ) callconv(.winapi) windows.HANDLE,
    .{ .name = "CreateFileW", .library_name = "kernel32" },
);

pub const get_system_time_precise_as_file_time = @extern(
    *const fn (*windows.FILETIME) callconv(.winapi) void,
    .{ .name = "GetSystemTimePreciseAsFileTime", .library_name = "kernel32" },
);

pub const get_process_times = @extern(
    *const fn (
        windows.HANDLE,
        *windows.FILETIME,
        *windows.FILETIME,
        *windows.FILETIME,
        *windows.FILETIME,
    ) callconv(.winapi) windows.BOOL,
    .{ .name = "GetProcessTimes", .library_name = "kernel32" },
);

pub const set_process_working_set_size = @extern(
    *const fn (windows.HANDLE, windows.SIZE_T, windows.SIZE_T) callconv(.winapi) windows.BOOL,
    .{ .name = "SetProcessWorkingSetSize", .library_name = "kernel32" },
);

pub const get_process_working_set_size = @extern(
    *const fn (windows.HANDLE, *windows.SIZE_T, *windows.SIZE_T) callconv(.winapi) windows.BOOL,
    .{ .name = "GetProcessWorkingSetSize", .library_name = "kernel32" },
);

pub const read_file_raw = @extern(
    *const fn (
        windows.HANDLE,
        [*]u8,
        windows.DWORD,
        *windows.DWORD,
        ?*anyopaque,
    ) callconv(.winapi) windows.BOOL,
    .{ .name = "ReadFile", .library_name = "kernel32" },
);

pub const write_file_raw = @extern(
    *const fn (
        windows.HANDLE,
        [*]const u8,
        windows.DWORD,
        *windows.DWORD,
        ?*anyopaque,
    ) callconv(.winapi) windows.BOOL,
    .{ .name = "WriteFile", .library_name = "kernel32" },
);

const wait_for_single_object_raw = @extern(
    *const fn (windows.HANDLE, windows.DWORD) callconv(.winapi) windows.DWORD,
    .{ .name = "WaitForSingleObject", .library_name = "kernel32" },
);

const create_io_completion_port_raw = @extern(
    *const fn (
        windows.HANDLE,
        ?windows.HANDLE,
        usize,
        windows.DWORD,
    ) callconv(.winapi) ?windows.HANDLE,
    .{ .name = "CreateIoCompletionPort", .library_name = "kernel32" },
);

const get_queued_completion_status_ex_raw = @extern(
    *const fn (
        windows.HANDLE,
        [*]OVERLAPPED_ENTRY,
        windows.ULONG,
        *windows.ULONG,
        windows.DWORD,
        windows.BOOL,
    ) callconv(.winapi) windows.BOOL,
    .{ .name = "GetQueuedCompletionStatusEx", .library_name = "kernel32" },
);

const query_performance_counter_raw = @extern(
    *const fn (*i64) callconv(.winapi) windows.BOOL,
    .{ .name = "QueryPerformanceCounter", .library_name = "kernel32" },
);

const query_performance_frequency_raw = @extern(
    *const fn (*i64) callconv(.winapi) windows.BOOL,
    .{ .name = "QueryPerformanceFrequency", .library_name = "kernel32" },
);

const set_file_pointer_ex_raw = @extern(
    *const fn (windows.HANDLE, i64, ?*i64, windows.DWORD) callconv(.winapi) windows.BOOL,
    .{ .name = "SetFilePointerEx", .library_name = "kernel32" },
);

pub const get_overlapped_result_raw = @extern(
    *const fn (
        windows.HANDLE,
        *OVERLAPPED,
        *windows.DWORD,
        windows.BOOL,
    ) callconv(.winapi) windows.BOOL,
    .{ .name = "GetOverlappedResult", .library_name = "kernel32" },
);

const wsa_socket_w_raw = @extern(
    *const fn (
        c_int,
        c_int,
        c_int,
        ?*anyopaque,
        windows.DWORD,
        windows.DWORD,
    ) callconv(.winapi) windows.HANDLE,
    .{ .name = "WSASocketW", .library_name = "ws2_32" },
);

pub const wsa_ioctl_raw = @extern(
    *const fn (
        windows.HANDLE,
        windows.DWORD,
        ?*anyopaque,
        windows.DWORD,
        ?*anyopaque,
        windows.DWORD,
        *windows.DWORD,
        ?*OVERLAPPED,
        ?*anyopaque,
    ) callconv(.winapi) c_int,
    .{ .name = "WSAIoctl", .library_name = "ws2_32" },
);

const set_file_completion_notification_modes_raw = @extern(
    *const fn (windows.HANDLE, u8) callconv(.winapi) windows.BOOL,
    .{ .name = "SetFileCompletionNotificationModes", .library_name = "kernel32" },
);

const connect_raw = @extern(
    *const fn (
        windows.HANDLE,
        *const std.posix.sockaddr,
        std.posix.socklen_t,
    ) callconv(.winapi) c_int,
    .{ .name = "connect", .library_name = "ws2_32" },
);

const closesocket_raw = @extern(
    *const fn (windows.HANDLE) callconv(.winapi) c_int,
    .{ .name = "closesocket", .library_name = "ws2_32" },
);

const bind_raw = @extern(
    *const fn (
        windows.HANDLE,
        *const std.posix.sockaddr,
        std.posix.socklen_t,
    ) callconv(.winapi) c_int,
    .{ .name = "bind", .library_name = "ws2_32" },
);

const listen_raw = @extern(
    *const fn (windows.HANDLE, c_int) callconv(.winapi) c_int,
    .{ .name = "listen", .library_name = "ws2_32" },
);

const getsockname_raw = @extern(
    *const fn (
        windows.HANDLE,
        *std.posix.sockaddr,
        *std.posix.socklen_t,
    ) callconv(.winapi) c_int,
    .{ .name = "getsockname", .library_name = "ws2_32" },
);

const setsockopt_raw = @extern(
    *const fn (windows.HANDLE, c_int, u32, [*]const u8, c_int) callconv(.winapi) c_int,
    .{ .name = "setsockopt", .library_name = "ws2_32" },
);

const shutdown_raw = @extern(
    *const fn (windows.HANDLE, c_int) callconv(.winapi) c_int,
    .{ .name = "shutdown", .library_name = "ws2_32" },
);

pub const wsa_get_overlapped_result_raw = @extern(
    *const fn (
        windows.HANDLE,
        *OVERLAPPED,
        *windows.DWORD,
        windows.BOOL,
        *windows.DWORD,
    ) callconv(.winapi) windows.BOOL,
    .{ .name = "WSAGetOverlappedResult", .library_name = "ws2_32" },
);

const getsockopt_raw = @extern(
    *const fn (windows.HANDLE, c_int, u32, [*]u8, *c_int) callconv(.winapi) c_int,
    .{ .name = "getsockopt", .library_name = "ws2_32" },
);

pub const wsa_send_raw = @extern(
    *const fn (
        windows.HANDLE,
        *WSABUF,
        windows.DWORD,
        *windows.DWORD,
        windows.DWORD,
        *OVERLAPPED,
        ?*anyopaque,
    ) callconv(.winapi) c_int,
    .{ .name = "WSASend", .library_name = "ws2_32" },
);

pub const wsa_recv_raw = @extern(
    *const fn (
        windows.HANDLE,
        *WSABUF,
        windows.DWORD,
        *windows.DWORD,
        *windows.DWORD,
        *OVERLAPPED,
        ?*anyopaque,
    ) callconv(.winapi) c_int,
    .{ .name = "WSARecv", .library_name = "ws2_32" },
);

pub const accept_ex = @extern(
    *const fn (
        windows.HANDLE,
        windows.HANDLE,
        [*]u8,
        windows.DWORD,
        windows.DWORD,
        windows.DWORD,
        *windows.DWORD,
        *OVERLAPPED,
    ) callconv(.winapi) windows.BOOL,
    .{ .name = "AcceptEx", .library_name = "mswsock" },
);

pub const SOCKET_ERROR: c_int = -1;
pub const SIO_GET_EXTENSION_FUNCTION_POINTER: windows.DWORD = 0xc8000006;
pub const WSAID_CONNECTEX = windows.GUID.parse("{25a207b9-ddf3-4660-8ee9-76e58c74063e}");

const wsa_get_last_error_raw = @extern(
    *const fn () callconv(.winapi) c_int,
    .{ .name = "WSAGetLastError", .library_name = "ws2_32" },
);

const get_console_mode_raw = @extern(
    *const fn (windows.HANDLE, *windows.DWORD) callconv(.winapi) windows.BOOL,
    .{ .name = "GetConsoleMode", .library_name = "kernel32" },
);

const set_console_mode_raw = @extern(
    *const fn (windows.HANDLE, windows.DWORD) callconv(.winapi) windows.BOOL,
    .{ .name = "SetConsoleMode", .library_name = "kernel32" },
);

pub const LOCKFILE_EXCLUSIVE_LOCK = 0x2;
pub const LOCKFILE_FAIL_IMMEDIATELY = 0x1;
pub const lock_file_ex = @extern(
    *const fn (
        windows.HANDLE,
        windows.DWORD,
        windows.DWORD,
        windows.DWORD,
        windows.DWORD,
        ?*anyopaque,
    ) callconv(.winapi) windows.BOOL,
    .{ .name = "LockFileEx", .library_name = "kernel32" },
);

pub const set_end_of_file = @extern(
    *const fn (windows.HANDLE) callconv(.winapi) windows.BOOL,
    .{ .name = "SetEndOfFile", .library_name = "kernel32" },
);

pub const connect_named_pipe = @extern(
    *const fn (windows.HANDLE, ?*anyopaque) callconv(.winapi) windows.BOOL,
    .{ .name = "ConnectNamedPipe", .library_name = "kernel32" },
);

pub fn random_bytes(buffer: []u8) void {
    if (!system_function_036(buffer.ptr, @intCast(buffer.len)).toBool()) {
        @panic("RtlGenRandom failed");
    }
}

pub fn query_performance_counter() u64 {
    var result: i64 = undefined;
    if (!query_performance_counter_raw(&result).toBool()) @panic("QueryPerformanceCounter failed");
    return @intCast(result);
}

pub fn query_performance_frequency() u64 {
    var result: i64 = undefined;
    if (!query_performance_frequency_raw(&result).toBool()) {
        @panic("QueryPerformanceFrequency failed");
    }
    return @intCast(result);
}

pub fn set_file_pointer_ex(handle: windows.HANDLE, offset: u64) std.posix.UnexpectedError!void {
    const offset_i: i64 = std.math.cast(i64, offset) orelse return error.Unexpected;
    if (!set_file_pointer_ex_raw(handle, offset_i, null, 0).toBool()) {
        return windows.unexpectedError(windows.GetLastError());
    }
}

pub fn wsa_socket_w(
    address_family: c_int,
    socket_type: c_int,
    protocol: c_int,
    protocol_info: ?*anyopaque,
    group: windows.DWORD,
    flags: windows.DWORD,
) std.posix.UnexpectedError!windows.HANDLE {
    const socket = wsa_socket_w_raw(
        address_family,
        socket_type,
        protocol,
        protocol_info,
        group,
        flags,
    );
    if (socket == windows.INVALID_HANDLE_VALUE) {
        return windows.unexpectedError(windows.GetLastError());
    }
    return socket;
}

pub fn set_file_completion_notification_modes(
    handle: windows.HANDLE,
    mode: u8,
) std.posix.UnexpectedError!void {
    if (!set_file_completion_notification_modes_raw(handle, mode).toBool()) {
        return windows.unexpectedError(windows.GetLastError());
    }
}

pub fn connect_socket(
    socket: windows.HANDLE,
    address: *const std.posix.sockaddr,
    address_len: std.posix.socklen_t,
) !void {
    if (connect_raw(socket, address, address_len) == 0) return;

    return switch (wsa_get_last_error_raw()) {
        10013 => error.AccessDenied,
        10048 => error.AddressInUse,
        10049 => error.AddressNotAvailable,
        10047 => error.AddressFamilyNotSupported,
        10037 => error.ConnectionPending,
        10009 => error.FileDescriptorNotASocket,
        10061 => error.ConnectionRefused,
        10035 => error.WouldBlock,
        10004 => error.Interrupted,
        10056 => error.AlreadyConnected,
        10051 => error.NetworkUnreachable,
        10091 => error.NetworkSubsystemFailed,
        10041 => error.ProtocolNotSupported,
        10060 => error.ConnectionTimedOut,
        else => error.Unexpected,
    };
}

pub fn close_socket(socket: windows.HANDLE) void {
    _ = closesocket_raw(socket);
}

pub fn bind_socket(
    socket: windows.HANDLE,
    address: *const std.posix.sockaddr,
    address_len: std.posix.socklen_t,
) !void {
    if (bind_raw(socket, address, address_len) == 0) return;

    return switch (wsa_get_last_error_raw()) {
        10013 => error.AccessDenied,
        10048 => error.AddressInUse,
        10049 => error.AddressNotAvailable,
        10047 => error.AddressFamilyNotSupported,
        10009 => error.FileDescriptorNotASocket,
        10050 => error.NetworkDown,
        10093 => error.NetworkSubsystemFailed,
        10041 => error.ProtocolNotSupported,
        else => error.Unexpected,
    };
}

pub fn listen_socket(socket: windows.HANDLE, backlog: u31) !void {
    if (listen_raw(socket, backlog) == 0) return;

    return switch (wsa_get_last_error_raw()) {
        10048 => error.AddressInUse,
        10009 => error.FileDescriptorNotASocket,
        10045 => error.OperationNotSupported,
        10050 => error.NetworkDown,
        10093 => error.NetworkSubsystemFailed,
        10055 => error.SystemResources,
        10022 => error.InvalidArgument,
        else => error.Unexpected,
    };
}

pub fn getsockname(
    socket: windows.HANDLE,
    address: *std.posix.sockaddr,
    address_len: *std.posix.socklen_t,
) !void {
    if (getsockname_raw(socket, address, address_len) == 0) return;

    return switch (wsa_get_last_error_raw()) {
        10009 => error.FileDescriptorNotASocket,
        10014 => error.AccessDenied,
        10022 => error.InvalidArgument,
        10050 => error.NetworkDown,
        10093 => error.NetworkSubsystemFailed,
        else => error.Unexpected,
    };
}

pub fn setsockopt(
    socket: windows.HANDLE,
    level: c_int,
    option: u32,
    value: []const u8,
) !void {
    if (setsockopt_raw(socket, level, option, value.ptr, @intCast(value.len)) == 0) return;

    return switch (wsa_get_last_error_raw()) {
        10013 => error.PermissionDenied,
        10009 => error.FileDescriptorNotASocket,
        10022 => error.InvalidArgument,
        10055 => error.SystemResources,
        10042 => error.ProtocolNotAvailable,
        else => error.Unexpected,
    };
}

pub fn setsockopt_null(socket: windows.HANDLE, level: c_int, option: u32) !void {
    if (setsockopt_raw(socket, level, option, undefined, 0) == 0) return;

    return switch (wsa_get_last_error_raw()) {
        10013 => error.PermissionDenied,
        10009 => error.FileDescriptorNotASocket,
        10022 => error.InvalidArgument,
        10055 => error.SystemResources,
        10042 => error.ProtocolNotAvailable,
        else => error.Unexpected,
    };
}

pub const ShutdownHow = enum { recv, send, both };
pub const ShutdownError = error{
    SocketUnconnected,
    ConnectionAborted,
    ConnectionResetByPeer,
    NetworkDown,
    SystemResources,
    Canceled,
} || std.posix.UnexpectedError;

pub fn shutdown_socket(socket: windows.HANDLE, how: ShutdownHow) ShutdownError!void {
    const how_c: c_int = switch (how) {
        .recv => 0,
        .send => 1,
        .both => 2,
    };
    if (shutdown_raw(socket, how_c) == 0) return;

    return switch (wsa_get_last_error_raw()) {
        10057 => error.SocketUnconnected,
        10053 => error.ConnectionAborted,
        10054 => error.ConnectionResetByPeer,
        10050 => error.NetworkDown,
        10055 => error.SystemResources,
        else => error.Unexpected,
    };
}

pub fn getsockopt(
    socket: windows.HANDLE,
    level: c_int,
    option: u32,
    value: []u8,
    size: *c_int,
) !void {
    if (getsockopt_raw(socket, level, option, value.ptr, size) == 0) return;

    return switch (wsa_get_last_error_raw()) {
        10050 => error.NetworkUnreachable,
        10009 => error.FileDescriptorNotASocket,
        else => error.Unexpected,
    };
}

pub fn wsa_get_last_error() WinsockError {
    return @enumFromInt(@as(u16, @intCast(wsa_get_last_error_raw())));
}

pub fn unexpected_wsa_error(err: WinsockError) std.posix.UnexpectedError {
    switch (@import("builtin").mode) {
        .Debug => std.debug.panic("unexpected Winsock error: {d} ({s})", .{
            @intFromEnum(err),
            std.enums.tagName(WinsockError, err) orelse "<unnamed>",
        }),
        else => return error.Unexpected,
    }
}

pub fn get_console_mode(handle: windows.HANDLE) std.posix.UnexpectedError!windows.DWORD {
    var mode: windows.DWORD = 0;
    if (!get_console_mode_raw(handle, &mode).toBool()) {
        return windows.unexpectedError(windows.GetLastError());
    }
    return mode;
}

pub fn set_console_mode(
    handle: windows.HANDLE,
    mode: windows.DWORD,
) std.posix.UnexpectedError!void {
    if (!set_console_mode_raw(handle, mode).toBool()) {
        return windows.unexpectedError(windows.GetLastError());
    }
}

pub fn wsa_startup() std.posix.UnexpectedError!void {
    var data: [wsadata_size]u8 = undefined;
    if (wsa_startup_raw(0x0202, &data) != 0) return error.Unexpected;
}

pub fn wsa_cleanup() std.posix.UnexpectedError!void {
    if (wsa_cleanup_raw() != 0) return error.Unexpected;
}

pub fn read_file(
    handle: windows.HANDLE,
    buffer: []u8,
    overlapped: ?*anyopaque,
) std.posix.UnexpectedError!usize {
    var bytes_read: windows.DWORD = undefined;
    if (!read_file_raw(
        handle,
        buffer.ptr,
        @intCast(buffer.len),
        &bytes_read,
        overlapped,
    ).toBool()) {
        return windows.unexpectedError(windows.GetLastError());
    }
    return bytes_read;
}

pub fn write_file(
    handle: windows.HANDLE,
    buffer: []const u8,
    overlapped: ?*anyopaque,
) std.posix.UnexpectedError!usize {
    var bytes_written: windows.DWORD = undefined;
    if (!write_file_raw(
        handle,
        buffer.ptr,
        @intCast(buffer.len),
        &bytes_written,
        overlapped,
    ).toBool()) {
        return windows.unexpectedError(windows.GetLastError());
    }
    return bytes_written;
}

pub fn flush_file_buffers(handle: windows.HANDLE) std.posix.UnexpectedError!void {
    if (!flush_file_buffers_raw(handle).toBool()) {
        return windows.unexpectedError(windows.GetLastError());
    }
}

pub fn get_file_size_ex(handle: windows.HANDLE) std.posix.UnexpectedError!u64 {
    var size: i64 = undefined;
    if (!get_file_size_ex_raw(handle, &size).toBool()) {
        return windows.unexpectedError(windows.GetLastError());
    }
    return @intCast(size);
}

pub fn sleep(milliseconds: windows.DWORD) void {
    sleep_raw(milliseconds);
}

pub fn wait_for_single_object(
    handle: windows.HANDLE,
    milliseconds: windows.DWORD,
) std.posix.UnexpectedError!void {
    switch (wait_for_single_object_raw(handle, milliseconds)) {
        0 => {}, // WAIT_OBJECT_0
        0x00000080 => {}, // WAIT_ABANDONED
        else => return windows.unexpectedError(windows.GetLastError()),
    }
}

pub fn create_io_completion_port(
    file_handle: windows.HANDLE,
    existing_completion_port: ?windows.HANDLE,
    completion_key: usize,
    number_of_concurrent_threads: windows.DWORD,
) std.posix.UnexpectedError!windows.HANDLE {
    return create_io_completion_port_raw(
        file_handle,
        existing_completion_port,
        completion_key,
        number_of_concurrent_threads,
    ) orelse windows.unexpectedError(windows.GetLastError());
}

pub const CompletionStatusError = error{ Timeout, Aborted } || std.posix.UnexpectedError;

pub fn get_queued_completion_status_ex(
    completion_port: windows.HANDLE,
    entries: []OVERLAPPED_ENTRY,
    timeout: windows.DWORD,
    alertable: bool,
) CompletionStatusError!u32 {
    var entries_removed: windows.ULONG = undefined;
    if (get_queued_completion_status_ex_raw(
        completion_port,
        entries.ptr,
        @intCast(entries.len),
        &entries_removed,
        timeout,
        windows.BOOL.fromBool(alertable),
    ).toBool()) {
        return entries_removed;
    }

    return switch (windows.GetLastError()) {
        .WAIT_TIMEOUT => error.Timeout,
        .ABANDONED_WAIT_0 => error.Aborted,
        else => |err| windows.unexpectedError(err),
    };
}
