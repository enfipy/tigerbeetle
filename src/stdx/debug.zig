const std = @import("std");
const time_units = @import("time_units.zig");

fn comptime_repeat(comptime pattern: []const u8, comptime count: usize) []const u8 {
    return comptime blk: {
        var output: [pattern.len * count]u8 = undefined;
        for (0..count) |i| {
            for (pattern, 0..) |byte, j| {
                output[i * pattern.len + j] = byte;
            }
        }
        const final = output;
        break :blk &final;
    };
}

/// Utility function for ad-hoc profiling.
///
/// A thin wrapper around `std.time.Timer` which handles the boilerplate of
/// printing to stderr and formatting times in some (unspecified) readable way.
pub fn timeit() TimeIt {
    return TimeIt{ .inner = std.time.Timer.start() catch unreachable };
}

const TimeIt = struct {
    inner: std.time.Timer,

    /// Prints elapsed time to stderr and resets the internal timer.
    pub fn print(self: *TimeIt, comptime label: []const u8) void {
        const label_alignment = comptime comptime_repeat(" ", 1 + (12 -| label.len));

        const elapsed_ns = self.inner.lap();
        std.debug.print(
            label ++ ":" ++ label_alignment ++ "{}\n",
            .{time_units.fmt_duration(elapsed_ns)},
        );
    }

    pub fn print_if_longer_than_ms(
        self: *TimeIt,
        threshold_ms: u64,
        comptime label: []const u8,
    ) void {
        self.if_longer_than(label, threshold_ms, false);
    }

    pub fn backtrace_if_longer_than_ms(
        self: *TimeIt,
        threshold_ms: u64,
        comptime label: []const u8,
    ) void {
        self.if_longer_than(label, threshold_ms, true);
    }

    fn if_longer_than(
        self: *TimeIt,
        comptime label: []const u8,
        threshold_ms: u64,
        backtrace: bool,
    ) void {
        const elapsed_ns = self.inner.lap();
        if (elapsed_ns > threshold_ms * std.time.ns_per_ms) {
            std.debug.print(label ++ ": {}\n", .{time_units.fmt_duration(elapsed_ns)});
            if (backtrace) std.debug.dumpCurrentStackTrace(null);
        }
    }
};

/// Utility for print-if debugging, a-la Rust's dbg! macro.
///
/// dbg prints the value with the prefix, while also returning the value, which makes it convenient
/// to drop it in the middle of a complex expression.
pub fn dbg(prefix: []const u8, value: anytype) @TypeOf(value) {
    std.debug.print("{s} = {any}\n", .{
        prefix,
        std.json.fmt(value, .{ .whitespace = .indent_2 }),
    });
    return value;
}
