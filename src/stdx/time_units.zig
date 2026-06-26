const std = @import("std");
const assert = std.debug.assert;
const stdx = @import("stdx.zig");
const builtin = @import("builtin");

extern "c" fn clock_gettime(clock_id: std.c.clockid_t, timespec: *std.c.timespec) c_int;

/// A moment in monotonic time not anchored to any particular epoch.
///
/// The absolute value of `ns` is meaningless, but it is possible to compute `Duration` between
/// two `Instant`s sourced from the same clock.
///
/// See also `InstantUnix`.
pub const Instant = struct {
    ns: u64,

    pub fn add(now: Instant, duration: Duration) Instant {
        return .{ .ns = now.ns + duration.ns };
    }

    pub fn elapsed(earlier: Instant, now: Instant) Duration {
        assert(now.ns >= earlier.ns);
        const elapsed_ns = now.ns - earlier.ns;
        return .{ .ns = elapsed_ns };
    }
};

/// Non-negative time difference between two `Instant`s.
pub const Duration = struct {
    ns: u64,

    pub fn us(amount_us: u64) Duration {
        return .{ .ns = amount_us * std.time.ns_per_us };
    }

    pub fn ms(amount_ms: u64) Duration {
        return .{ .ns = amount_ms * std.time.ns_per_ms };
    }

    pub fn seconds(amount_seconds: u64) Duration {
        return .{ .ns = amount_seconds * std.time.ns_per_s };
    }

    pub fn minutes(amount_minutes: u64) Duration {
        return .{ .ns = amount_minutes * std.time.ns_per_min };
    }

    // Duration in microseconds, μs, 1/1_000_000 of a second.
    pub fn to_us(duration: Duration) u64 {
        return @divFloor(duration.ns, std.time.ns_per_us);
    }

    // Duration in milliseconds, ms, 1/1_000 of a second.
    pub fn to_ms(duration: Duration) u64 {
        return @divFloor(duration.ns, std.time.ns_per_ms);
    }

    pub fn min(lhs: Duration, rhs: Duration) Duration {
        return .{ .ns = @min(lhs.ns, rhs.ns) };
    }

    pub fn max(lhs: Duration, rhs: Duration) Duration {
        return .{ .ns = @max(lhs.ns, rhs.ns) };
    }

    pub fn clamp(duration: Duration, clamp_min: Duration, clamp_max: Duration) Duration {
        assert(clamp_min.ns <= clamp_max.ns);
        if (duration.ns < clamp_min.ns) return clamp_min;
        if (duration.ns > clamp_max.ns) return clamp_max;
        return duration;
    }

    pub const sort = struct {
        pub fn asc(ctx: void, lhs: Duration, rhs: Duration) bool {
            return std.sort.asc(u64)(ctx, lhs.ns, rhs.ns);
        }
    };

    // Human readable format like `1.123s`.
    // NB: this is a lossy operation, durations are rounded to look nice.
    pub fn format(duration: Duration, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.print("{f}", .{fmt_duration(duration.ns)});
    }

    pub fn parse_flag_value(
        string: []const u8,
        static_diagnostic: *?[]const u8,
    ) error{InvalidFlagValue}!Duration {
        assert(string.len > 0);
        var string_remaining = string;

        var result: Duration = .{ .ns = 0 };
        while (string_remaining.len > 0) {
            string_remaining, const component =
                try parse_flag_value_component(string_remaining, static_diagnostic);
            result.ns +|= component.ns;
        }

        if (result.ns >= 1_000 * std.time.ns_per_day) {
            static_diagnostic.* = "duration too large:";
            return error.InvalidFlagValue;
        }
        return result;
    }

    fn parse_flag_value_component(
        string: []const u8,
        static_diagnostic: *?[]const u8,
    ) error{InvalidFlagValue}!struct { []const u8, Duration } {
        const split_index = for (string, 0..) |c, index| {
            if (std.ascii.isDigit(c)) {
                // Numeric part continues.
            } else break index;
        } else {
            static_diagnostic.* = "missing unit; must be one of: d/h/m/s/ms/us/ns:";
            return error.InvalidFlagValue;
        };

        if (split_index == 0) {
            static_diagnostic.* = "missing value:";
            return error.InvalidFlagValue;
        }

        const string_amount = string[0..split_index];
        const string_remaining = string[split_index..];
        assert(string_amount.len > 0);
        assert(string_remaining.len > 0);

        const amount = stdx.parse_int(u64, string_amount, .{
            .base = 10,
            .allow_separators = true,
        }) catch |err| switch (err) {
            error.Overflow => {
                static_diagnostic.* = "integer overflow:";
                return error.InvalidFlagValue;
            },
            error.LeadingZero => {
                static_diagnostic.* = "leading zero disallowed:";
                return error.InvalidFlagValue;
            },
            error.InvalidCharacter => unreachable,
        };

        const Unit = enum(u64) {
            ns = 1,
            us = std.time.ns_per_us,
            ms = std.time.ns_per_ms,
            s = std.time.ns_per_s,
            m = std.time.ns_per_min,
            h = std.time.ns_per_hour,
            d = std.time.ns_per_day,
        };

        inline for (comptime std.enums.values(Unit)) |unit| {
            if (stdx.cut_prefix(string_remaining, @tagName(unit))) |suffix| {
                return .{ suffix, .{ .ns = amount *| @intFromEnum(unit) } };
            }
        } else {
            static_diagnostic.* = "unknown unit; must be one of: d/h/m/s/ms/us/ns:";
            return error.InvalidFlagValue;
        }
    }
};

test "Instant/Duration" {
    const instant_1: Instant = .{ .ns = 100 * std.time.ns_per_day };
    const instant_2: Instant = .{ .ns = 100 * std.time.ns_per_day + std.time.ns_per_s };
    assert(instant_1.elapsed(instant_1).ns == 0);
    assert(instant_1.elapsed(instant_2).ns == std.time.ns_per_s);

    const duration = instant_1.elapsed(instant_2);
    assert(duration.ns == 1_000_000_000);
    assert(duration.to_us() == 1_000_000);
    assert(duration.to_ms() == 1_000);

    assert(Duration.ms(1).ns == std.time.ns_per_ms);
    assert(Duration.seconds(1).ns == std.time.ns_per_s);
    assert(Duration.minutes(1).ns == std.time.ns_per_min);
}

test "Duration.parse_flag_value" {
    try stdx.Flags.parse_flag_value_fuzz(Duration, Duration.parse_flag_value, .{
        .ok = &.{
            .{ "1h", .{ .ns = std.time.ns_per_hour } },
            .{ "1m", .{ .ns = std.time.ns_per_min } },
            .{ "1h2m", .{ .ns = std.time.ns_per_hour + 2 * std.time.ns_per_min } },
            .{ "1ms2us3ns", .{ .ns = std.time.ns_per_ms + 2 * std.time.ns_per_us + 3 } },
        },
        .err = &.{
            .{ "h", "missing value" },
            .{ "1", "missing unit" },
            .{ "h1", "missing value" },
            .{ "1H", "unknown unit; must be one of: d/h/m/s/ms/us/ns" },
            .{ "1h2x", "unknown unit" },
            .{ "1_0h", "unknown unit" },
            .{ "1h 2m", "missing value" },
            .{ "18446744073709551616ns", "integer overflow" },
            .{ "1844674407370955161s", "duration too large" },
            .{ "0024h", "leading zero disallowed" },
        },
    });
}

/// A moment in non-monotonic Unix time.
/// Timestamp is relative to epoch 1970-01-1.
///
/// See also `Instant`.
pub fn fmt_duration(ns: u64) std.fmt.Alt(u64, format_duration) {
    return .{ .data = ns };
}

pub fn fmt_duration_signed(ns: i64) std.fmt.Alt(i64, format_duration_signed) {
    return .{ .data = ns };
}

fn format_duration(ns: u64, writer: *std.Io.Writer) std.Io.Writer.Error!void {
    if (ns < std.time.ns_per_us) {
        return writer.print("{}ns", .{ns});
    } else if (ns < std.time.ns_per_ms) {
        return format_duration_decimal(writer, ns, std.time.ns_per_us, "us");
    } else if (ns < std.time.ns_per_s) {
        return format_duration_decimal(writer, ns, std.time.ns_per_ms, "ms");
    } else {
        return format_duration_decimal(writer, ns, std.time.ns_per_s, "s");
    }
}

fn format_duration_signed(ns: i64, writer: *std.Io.Writer) std.Io.Writer.Error!void {
    const magnitude: u64 = if (ns < 0) negative: {
        try writer.writeByte('-');
        break :negative @as(u64, @intCast(-(ns + 1))) + 1;
    } else @intCast(ns);

    return format_duration(magnitude, writer);
}

fn format_duration_decimal(
    writer: *std.Io.Writer,
    ns: u64,
    unit: u64,
    suffix: []const u8,
) std.Io.Writer.Error!void {
    const whole = ns / unit;
    const fractional = (ns % unit) * 1000 / unit;

    if (fractional == 0) {
        return writer.print("{}{s}", .{ whole, suffix });
    } else {
        return writer.print("{}.{:0>3}{s}", .{ whole, fractional, suffix });
    }
}

pub const InstantUnix = struct {
    ns: u64,

    pub fn add(instant: InstantUnix, duration: Duration) InstantUnix {
        return .{ .ns = instant.ns + duration.ns };
    }

    pub fn now() InstantUnix {
        const timestamp_ns = switch (builtin.os.tag) {
            .linux => timestamp_ns_linux(),
            .driverkit,
            .ios,
            .maccatalyst,
            .macos,
            .tvos,
            .visionos,
            .watchos,
            => timestamp_ns_posix(),
            .windows => timestamp_ns_windows(),
            else => @compileError("unsupported OS"),
        };
        assert(timestamp_ns > 0);
        assert(timestamp_ns <= std.math.maxInt(u64));
        return .{ .ns = @intCast(timestamp_ns) };
    }

    fn timestamp_ns_linux() i128 {
        var timespec: std.os.linux.timespec = undefined;
        assert(std.os.linux.clock_gettime(.REALTIME, &timespec) == 0);
        return @as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec;
    }

    fn timestamp_ns_posix() i128 {
        var timespec: std.c.timespec = undefined;
        assert(clock_gettime(std.c.CLOCK.REALTIME, &timespec) == 0);
        return @as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec;
    }

    fn timestamp_ns_windows() i128 {
        var file_time: std.os.windows.FILETIME = undefined;
        stdx.windows.get_system_time_precise_as_file_time(&file_time);
        const ticks = (@as(u64, file_time.dwHighDateTime) << 32) | file_time.dwLowDateTime;
        const windows_to_unix_epoch_100ns = 11644473600 * 10_000_000;
        assert(ticks >= windows_to_unix_epoch_100ns);
        return @as(i128, ticks - windows_to_unix_epoch_100ns) * 100;
    }

    pub fn from_timestamp_s(timestamp_s: u64) InstantUnix {
        return InstantUnix{ .ns = timestamp_s * std.time.ms_per_s * std.time.ns_per_ms };
    }

    pub fn date_time(instant: InstantUnix) struct {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        millisecond: u16,
    } {
        const timestamp_ms = @divTrunc(instant.ns, std.time.ns_per_ms);
        const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = @divTrunc(timestamp_ms, 1000) };
        const year_day = epoch_seconds.getEpochDay().calculateYearDay();
        const month_day = year_day.calculateMonthDay();
        const time = epoch_seconds.getDaySeconds();

        return .{
            .year = year_day.year,
            .month = month_day.month.numeric(),
            .day = month_day.day_index + 1,
            .hour = time.getHoursIntoDay(),
            .minute = time.getMinutesIntoHour(),
            .second = time.getSecondsIntoMinute(),
            .millisecond = @intCast(@mod(timestamp_ms, 1000)),
        };
    }

    pub fn format(instant: InstantUnix, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        const datetime = instant.date_time();
        try writer.print("{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
            datetime.year,
            datetime.month,
            datetime.day,
            datetime.hour,
            datetime.minute,
            datetime.second,
            datetime.millisecond,
        });
    }

    pub fn to_seconds(instant: InstantUnix) u64 {
        return @divFloor(instant.ns, std.time.ns_per_s);
    }
};

test "InstantUnix format" {
    const instant_min = InstantUnix{ .ns = 0 };
    var buffer: [24]u8 = undefined;
    try std.testing.expectEqualStrings(
        "1970-01-01 00:00:00.000Z",
        try std.fmt.bufPrint(&buffer, "{f}", .{instant_min}),
    );
    const instant_max = InstantUnix{ .ns = std.math.maxInt(u64) };
    try std.testing.expectEqualStrings(
        "2554-07-21 23:34:33.709Z",
        try std.fmt.bufPrint(&buffer, "{f}", .{instant_max}),
    );
}
