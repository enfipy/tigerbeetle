//! Code shared across several IO implementations, because, e.g., it is expressible via POSIX layer.
const builtin = @import("builtin");
const std = @import("std");
const posix = std.posix;

const stdx = @import("stdx");

const Tracer = @import("../trace.zig").Tracer;

const assert = std.debug.assert;

const is_linux = builtin.target.os.tag == .linux;

pub const TCPOptions = struct {
    rcvbuf: c_int,
    sndbuf: c_int,
    keepalive: ?struct {
        keepidle: c_int,
        keepintvl: c_int,
        keepcnt: c_int,
    },
    user_timeout_ms: c_int,
    nodelay: bool,
};

pub const ListenOptions = struct {
    backlog: u31,
};

pub const NextTickSource = enum { lsm, vsr };

pub fn listen(
    fd: posix.socket_t,
    address: std.Io.net.IpAddress,
    options: ListenOptions,
) !std.Io.net.IpAddress {
    try setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, 1);

    if (builtin.os.tag != .linux) @compileError("listen is implemented only for Linux");

    const sockaddr, const sockaddr_len = stdx.ip_address_to_sockaddr(address);
    switch (os.linux.errno(os.linux.bind(
        fd,
        &sockaddr.any,
        sockaddr_len,
    ))) {
        .SUCCESS => {},
        .ACCES => return error.AccessDenied,
        .ADDRINUSE => return error.AddressInUse,
        .BADF => unreachable,
        .INVAL => return error.Unexpected,
        .NOTSOCK => unreachable,
        else => |err| return stdx.unexpected_errno("bind", err),
    }

    // Resolve port 0 to an actual port picked by the OS.
    var sockaddr_resolved: stdx.PosixAddress = undefined;
    var sockaddr_resolved_len: posix.socklen_t = @sizeOf(stdx.PosixAddress);
    switch (os.linux.errno(os.linux.getsockname(
        fd,
        &sockaddr_resolved.any,
        &sockaddr_resolved_len,
    ))) {
        .SUCCESS => {},
        .BADF => unreachable,
        .FAULT => unreachable,
        .INVAL => unreachable,
        .NOTSOCK => unreachable,
        else => |err| return stdx.unexpected_errno("getsockname", err),
    }
    const address_resolved = stdx.sockaddr_to_ip_address(&sockaddr_resolved);
    assert(std.meta.activeTag(address_resolved) == std.meta.activeTag(address));

    switch (os.linux.errno(os.linux.listen(fd, options.backlog))) {
        .SUCCESS => {},
        .ADDRINUSE => return error.AddressInUse,
        .BADF => unreachable,
        .NOTSOCK => unreachable,
        .OPNOTSUPP => return error.OperationNotSupported,
        else => |err| return stdx.unexpected_errno("listen", err),
    }

    return address_resolved;
}

/// Sets the socket options.
/// Although some options are generic at the socket level,
/// these settings are intended only for TCP sockets.
pub fn tcp_options(
    fd: posix.socket_t,
    options: TCPOptions,
) !void {
    if (options.rcvbuf > 0) rcvbuf: {
        if (is_linux) {
            // Requires CAP_NET_ADMIN privilege (settle for SO_RCVBUF in case of an EPERM):
            if (setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVBUFFORCE, options.rcvbuf)) |_| {
                break :rcvbuf;
            } else |err| switch (err) {
                error.PermissionDenied => {},
                else => |e| return e,
            }
        }
        try setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, options.rcvbuf);
    }

    if (options.sndbuf > 0) sndbuf: {
        if (is_linux) {
            // Requires CAP_NET_ADMIN privilege (settle for SO_SNDBUF in case of an EPERM):
            if (setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDBUFFORCE, options.sndbuf)) |_| {
                break :sndbuf;
            } else |err| switch (err) {
                error.PermissionDenied => {},
                else => |e| return e,
            }
        }
        try setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDBUF, options.sndbuf);
    }

    if (options.keepalive) |keepalive| {
        try setsockopt(fd, posix.SOL.SOCKET, posix.SO.KEEPALIVE, 1);
        if (is_linux) {
            try setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.KEEPIDLE, keepalive.keepidle);
            try setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.KEEPINTVL, keepalive.keepintvl);
            try setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.KEEPCNT, keepalive.keepcnt);
        }
    }

    if (options.user_timeout_ms > 0) {
        if (is_linux) {
            const timeout_ms = options.user_timeout_ms;
            try setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.USER_TIMEOUT, timeout_ms);
        }
    }

    // Set tcp no-delay
    if (options.nodelay) {
        if (is_linux) {
            try setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.NODELAY, 1);
        }
    }
}

pub fn setsockopt(fd: posix.socket_t, level: i32, option: u32, value: c_int) !void {
    if (builtin.target.os.tag == .windows) {
        return stdx.windows.setsockopt(fd, level, option, &std.mem.toBytes(value));
    }
    try posix.setsockopt(fd, level, option, &std.mem.toBytes(value));
}

pub const AOFStat = struct {
    inode: u64,
    size: u64,
};

pub const AOFError = error{
    AccessDenied,
    FileBusy,
    FileNotFound,
    FileTooBig,
    InputOutput,
    Interrupted,
    IsDir,
    NameTooLong,
    NoSpaceLeft,
    NotDir,
    PathAlreadyExists,
    PermissionDenied,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    Unexpected,
    WouldBlock,
};

const os = std.os;

pub fn aof_blocking_write_all(fd: posix.fd_t, buffer: []const u8) AOFError!void {
    if (builtin.os.tag != .linux) @compileError("AOF blocking write is implemented only for Linux");

    var remaining = buffer;
    while (remaining.len > 0) {
        const count = @min(remaining.len, 0x7ffff000);
        const rc = os.linux.write(fd, remaining.ptr, count);
        switch (os.linux.errno(rc)) {
            .SUCCESS => {
                assert(rc > 0);
                remaining = remaining[rc..];
            },
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.Unexpected,
            .FAULT => unreachable,
            .FBIG => return error.FileTooBig,
            .INVAL => return error.Unexpected,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .PERM => return error.PermissionDenied,
            .PIPE => return error.Unexpected,
            else => |err| return stdx.unexpected_errno("write", err),
        }
    }
}

pub fn aof_blocking_pread_all(fd: posix.fd_t, buffer: []u8, offset: u64) AOFError!usize {
    if (builtin.os.tag != .linux) @compileError("AOF blocking pread is implemented only for Linux");

    var total: usize = 0;
    while (total < buffer.len) {
        const count = @min(buffer.len - total, 0x7ffff000);
        const rc = os.linux.pread(
            fd,
            buffer[total..].ptr,
            count,
            @intCast(offset + total),
        );
        switch (os.linux.errno(rc)) {
            .SUCCESS => {
                if (rc == 0) return total;
                total += rc;
            },
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.Unexpected,
            .FAULT => unreachable,
            .INVAL => return error.Unexpected,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .OVERFLOW => return error.FileTooBig,
            .NXIO => return error.Unexpected,
            else => |err| return stdx.unexpected_errno("pread", err),
        }
    }

    return total;
}

pub fn aof_blocking_close(fd: posix.fd_t) void {
    if (builtin.os.tag != .linux) @compileError("AOF blocking close is implemented only for Linux");

    stdx.close_fd(fd);
}

pub fn aof_blocking_stat(path: []const u8) AOFError!AOFStat {
    if (builtin.os.tag != .linux) @compileError("AOF blocking stat is implemented only for Linux");

    const path_z = posix.toPosixPath(path) catch return error.NameTooLong;
    return aof_blocking_statx(posix.AT.FDCWD, &path_z, 0);
}

pub fn aof_blocking_fstat(fd: posix.fd_t) AOFError!AOFStat {
    if (builtin.os.tag != .linux) @compileError("AOF blocking fstat is implemented only for Linux");

    return aof_blocking_statx(fd, "", os.linux.AT.EMPTY_PATH);
}

pub fn aof_blocking_open_read_only(dir_fd: posix.fd_t, path: []const u8) AOFError!posix.fd_t {
    assert(!std.fs.path.isAbsolute(path));

    if (builtin.os.tag != .linux) {
        @compileError("AOF blocking open read-only is implemented only for Linux");
    }

    const path_z = posix.toPosixPath(path) catch return error.NameTooLong;
    const flags: os.linux.O = .{
        .ACCMODE = .RDONLY,
        .CLOEXEC = true,
    };

    while (true) {
        const rc = os.linux.openat(dir_fd, &path_z, flags, 0);
        switch (os.linux.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .ACCES => return error.AccessDenied,
            .BADF => unreachable,
            .BUSY => return error.FileBusy,
            .FBIG, .OVERFLOW => return error.FileTooBig,
            .INVAL => return error.Unexpected,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .LOOP => return error.Unexpected,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NAMETOOLONG => return error.NameTooLong,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .PERM => return error.PermissionDenied,
            .ROFS => return error.AccessDenied,
            .TXTBSY => return error.FileBusy,
            else => |err| return stdx.unexpected_errno("openat", err),
        }
    }
}

pub fn aof_blocking_open(dir_fd: posix.fd_t, path: []const u8) !posix.fd_t {
    assert(!std.fs.path.isAbsolute(path));

    if (builtin.os.tag != .linux) @compileError("AOF blocking open is implemented only for Linux");

    const path_z = posix.toPosixPath(path) catch return error.NameTooLong;
    const flags: os.linux.O = .{
        .ACCMODE = .RDWR,
        .CREAT = true,
        .CLOEXEC = true,
    };
    const fd: posix.fd_t = fd: while (true) {
        const rc = os.linux.openat(dir_fd, &path_z, flags, 0o666);
        switch (os.linux.errno(rc)) {
            .SUCCESS => break :fd @intCast(rc),
            .INTR => continue,
            .ACCES => return error.AccessDenied,
            .BADF => unreachable,
            .BUSY => return error.FileBusy,
            .EXIST => return error.PathAlreadyExists,
            .FBIG, .OVERFLOW => return error.FileTooBig,
            .INVAL => return error.Unexpected,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .LOOP => return error.Unexpected,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NAMETOOLONG => return error.NameTooLong,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOENT => return error.FileNotFound,
            .NOSPC => return error.NoSpaceLeft,
            .NOTDIR => return error.NotDir,
            .PERM => return error.PermissionDenied,
            .ROFS => return error.AccessDenied,
            .TXTBSY => return error.FileBusy,
            else => |err| return stdx.unexpected_errno("openat", err),
        }
    };
    errdefer aof_blocking_close(fd);

    // Preserve the old exclusive-lock semantics from std.fs.Dir.createFile(..., .lock=.exclusive).
    switch (os.linux.errno(os.linux.flock(fd, 2))) {
        .SUCCESS => {},
        .INTR => return error.Interrupted,
        .BADF => unreachable,
        .INVAL => unreachable,
        .NOLCK => return error.Unexpected,
        else => |err| return stdx.unexpected_errno("flock", err),
    }

    switch (os.linux.errno(os.linux.fsync(fd))) {
        .SUCCESS => {},
        .BADF => unreachable,
        .INTR => return error.Interrupted,
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        else => |err| return stdx.unexpected_errno("fsync", err),
    }

    switch (os.linux.errno(os.linux.fsync(dir_fd))) {
        .SUCCESS => {},
        .BADF => unreachable,
        .INTR => return error.Interrupted,
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        else => |err| return stdx.unexpected_errno("fsync", err),
    }

    switch (os.linux.errno(os.linux.lseek(fd, 0, os.linux.SEEK.END))) {
        .SUCCESS => {},
        .BADF => unreachable,
        .INVAL => return error.Unexpected,
        .OVERFLOW => return error.FileTooBig,
        .SPIPE => return error.Unexpected,
        else => |err| return stdx.unexpected_errno("lseek", err),
    }

    return fd;
}

fn aof_blocking_statx(dir_fd: posix.fd_t, path: [*:0]const u8, flags: u32) AOFError!AOFStat {
    var statx_buffer: os.linux.Statx = undefined;
    switch (os.linux.errno(os.linux.statx(
        dir_fd,
        path,
        flags,
        os.linux.STATX.BASIC_STATS,
        &statx_buffer,
    ))) {
        .SUCCESS => return .{
            .inode = statx_buffer.ino,
            .size = statx_buffer.size,
        },
        .ACCES => return error.AccessDenied,
        .BADF => unreachable,
        .FAULT => unreachable,
        .INVAL => return error.Unexpected,
        .LOOP => return error.Unexpected,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOMEM => return error.Unexpected,
        .NOTDIR => return error.NotDir,
        .PERM => return error.PermissionDenied,
        else => |err| return stdx.unexpected_errno("statx", err),
    }
}

pub const Stats = struct {
    tracer: ?*Tracer = null,

    total: Timings = .{},
    window: Timings = .{},

    const Timings = struct {
        time_callbacks: stdx.Duration = .ms(0),
        time_run_for_ns: stdx.Duration = .ms(0),
        time_kernel: stdx.Duration = .ms(0),

        pub fn add(total: *Timings, increment: Timings) void {
            total.time_callbacks.ns +|= increment.time_callbacks.ns;
            total.time_run_for_ns.ns +|= increment.time_run_for_ns.ns;
            total.time_kernel.ns +|= increment.time_kernel.ns;
        }
    };

    pub fn trace(stats: *Stats) void {
        if (stats.tracer) |tracer| {
            tracer.timing(.loop_run_for_ns, stats.window.time_run_for_ns);
            tracer.timing(.loop_callbacks, stats.window.time_callbacks);
            tracer.timing(.loop_kernel, stats.window.time_kernel);
        }
        stats.total.add(stats.window);
        stats.window = .{};
    }
};
