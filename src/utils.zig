const std = @import("std");

pub fn backoffHelper(spins: u64) void {
    if (spins < 16) {
        std.atomic.spinLoopHint();
    } else {
        const level: u6 = @min(9, @as(u6, @truncate(spins - 16)));
        const base: u64 = 1;
        std.Thread.sleep((base << level) * std.time.ns_per_us);
    }
}
