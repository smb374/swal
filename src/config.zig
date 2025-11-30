const std = @import("std");
const record = @import("record.zig");
const RetentionParam = @import("ctx.zig").RetentionParam;

pub const WALConfig = struct {
    dir: []const u8,
    segment_max_size: u64,
    payload_hsah_type: record.PayloadHashType,
    retention: RetentionParam,
    buffer_size: u32,
};
