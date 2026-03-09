//! Code shared across several IO implementations, because, e.g., it is expressible via POSIX layer.
const builtin = @import("builtin");
const std = @import("std");
const posix = std.posix;
const stdx = @import("stdx");

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

pub const ListenError = posix.SetSockOptError || posix.UnexpectedError;

pub fn listen(
    fd: posix.socket_t,
    address: std.Io.net.IpAddress,
    options: ListenOptions,
) ListenError!std.Io.net.IpAddress {
    try setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, 1);

    var bind_storage: std.Io.Threaded.PosixAddress = undefined;
    const bind_len = std.Io.Threaded.addressToPosix(&address, &bind_storage);
    while (true) {
        switch (if (builtin.os.tag == .linux)
            posix.errno(std.os.linux.bind(fd, &bind_storage.any, bind_len))
        else
            posix.errno(std.c.bind(fd, &bind_storage.any, bind_len))) {
            .SUCCESS => break,
            .INTR => continue,
            else => |err| return stdx.unexpected_errno("bind", err),
        }
    }

    // Resolve port 0 to an actual port picked by the OS.
    var address_storage: std.Io.Threaded.PosixAddress = undefined;
    var addrlen: posix.socklen_t = @sizeOf(std.Io.Threaded.PosixAddress);
    while (true) {
        switch (if (builtin.os.tag == .linux)
            posix.errno(std.os.linux.getsockname(fd, &address_storage.any, &addrlen))
        else
            posix.errno(std.c.getsockname(fd, &address_storage.any, &addrlen))) {
            .SUCCESS => break,
            .INTR => continue,
            else => |err| return stdx.unexpected_errno("getsockname", err),
        }
    }

    const address_resolved = std.Io.Threaded.addressFromPosix(&address_storage);
    assert(std.Io.Threaded.posixAddressFamily(&address_resolved) ==
        std.Io.Threaded.posixAddressFamily(&address));

    while (true) {
        switch (if (builtin.os.tag == .linux)
            posix.errno(std.os.linux.listen(fd, options.backlog))
        else
            posix.errno(std.c.listen(fd, @intCast(options.backlog)))) {
            .SUCCESS => break,
            .INTR => continue,
            else => |err| return stdx.unexpected_errno("listen", err),
        }
    }

    return address_resolved;
}

/// Sets the socket options.
/// Although some options are generic at the socket level,
/// these settings are intended only for TCP sockets.
pub fn tcp_options(
    fd: posix.socket_t,
    options: TCPOptions,
) posix.SetSockOptError!void {
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

pub fn setsockopt(
    fd: posix.socket_t,
    level: i32,
    option: u32,
    value: c_int,
) posix.SetSockOptError!void {
    try posix.setsockopt(fd, level, option, &std.mem.toBytes(value));
}

pub const AOFBlockingWriteError = error{
    WouldBlock,
    NotOpenForWriting,
    NotConnected,
    DiskQuota,
    FileTooBig,
    Alignment,
    InputOutput,
    NoSpaceLeft,
    Unseekable,
    AccessDenied,
    BrokenPipe,
} || posix.UnexpectedError;

pub fn aof_blocking_write_all(fd: posix.fd_t, buffer: []const u8) AOFBlockingWriteError!void {
    var offset: usize = 0;
    while (offset < buffer.len) {
        const n = if (builtin.os.tag == .linux)
            std.os.linux.write(fd, buffer[offset..].ptr, buffer.len - offset)
        else
            std.c.write(fd, buffer[offset..].ptr, buffer.len - offset);
        switch (posix.errno(n)) {
            .SUCCESS => offset += @intCast(n),
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForWriting,
            .DESTADDRREQ => return error.NotConnected,
            .DQUOT => return error.DiskQuota,
            .FBIG => return error.FileTooBig,
            .INVAL => return error.Alignment,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .SPIPE => return error.Unseekable,
            .ACCES, .PERM => return error.AccessDenied,
            .PIPE => return error.BrokenPipe,
            else => |err| return stdx.unexpected_errno("write", err),
        }
    }
}

pub const AOFBlockingPReadError = error{
    WouldBlock,
    NotOpenForReading,
    ConnectionResetByPeer,
    Alignment,
    InputOutput,
    IsDir,
    SystemResources,
    Unseekable,
    ConnectionTimedOut,
} || posix.UnexpectedError;

pub fn aof_blocking_pread_all(
    fd: posix.fd_t,
    buffer: []u8,
    offset: u64,
) AOFBlockingPReadError!usize {
    var read_total: usize = 0;
    while (read_total < buffer.len) {
        const n = if (builtin.os.tag == .linux)
            std.os.linux.pread(
                fd,
                buffer[read_total..].ptr,
                buffer.len - read_total,
                @intCast(offset + read_total),
            )
        else
            std.c.pread(
                fd,
                buffer[read_total..].ptr,
                buffer.len - read_total,
                @intCast(offset + read_total),
            );
        switch (posix.errno(n)) {
            .SUCCESS => {
                if (n == 0) break;
                read_total += @intCast(n);
            },
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForReading,
            .CONNRESET => return error.ConnectionResetByPeer,
            .INVAL => return error.Alignment,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .NOBUFS, .NOMEM => return error.SystemResources,
            .NXIO, .OVERFLOW, .SPIPE => return error.Unseekable,
            .TIMEDOUT => return error.ConnectionTimedOut,
            else => |err| return stdx.unexpected_errno("pread", err),
        }
    }
    return read_total;
}

pub fn aof_blocking_close(fd: posix.fd_t) void {
    (std.Io.File{ .handle = fd, .flags = .{ .nonblocking = false } }).close(std.Options.debug_io);
}

pub const AOFBlockingStatError = std.Io.Dir.StatFileError;

pub fn aof_blocking_stat(path: []const u8) AOFBlockingStatError!std.Io.File.Stat {
    return std.Io.Dir.statFile(std.Io.Dir.cwd(), std.Options.debug_io, path, .{});
}

pub const AOFBlockingFStatError = std.Io.File.StatError;

pub fn aof_blocking_fstat(fd: posix.fd_t) AOFBlockingFStatError!std.Io.File.Stat {
    const file: std.Io.File = .{
        .handle = fd,
        .flags = .{ .nonblocking = false },
    };
    return file.stat(std.Options.debug_io);
}

pub const AOFBlockingOpenError = posix.OpenError || std.Io.File.SyncError || posix.UnexpectedError;

pub fn aof_blocking_open(dir_fd: posix.fd_t, path: []const u8) AOFBlockingOpenError!posix.fd_t {
    assert(!std.fs.path.isAbsolute(path));

    const fd = try posix.openat(dir_fd, path, .{
        .CREAT = true,
        .CLOEXEC = true,
        .ACCMODE = .RDWR,
    }, 0o666);
    errdefer (std.Io.File{ .handle = fd, .flags = .{ .nonblocking = false } }).close(std.Options.debug_io);

    try (std.Io.File{ .handle = fd, .flags = .{ .nonblocking = false } }).sync(std.Options.debug_io);

    // We cannot fsync the directory handle on Windows.
    // We have no way to open a directory with write access.
    if (builtin.os.tag != .windows) {
        try (std.Io.File{ .handle = dir_fd, .flags = .{ .nonblocking = false } }).sync(std.Options.debug_io);
    }

    const seek_rc = std.os.linux.lseek(fd, 0, std.c.SEEK.END);
    switch (posix.errno(seek_rc)) {
        .SUCCESS => {},
        else => |err| return stdx.unexpected_errno("lseek", err),
    }

    return fd;
}
