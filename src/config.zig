const std = @import("std");
const record = @import("record.zig");
const RetentionParam = @import("ctx.zig").RetentionParam;

pub const WAL_DEFAULT_SEGMENT_MAX: u64 = 1073741824;
pub const WAL_DEFAULT_BUFFER_SIZE: u32 = 16384;

pub const WALConfig = struct {
    dir: []const u8,
    segment_max_size: u64,
    payload_hash_type: record.PayloadHashType,
    retention: RetentionParam,
    buffer_size: u32,

    pub fn default(dir: []const u8) WALConfig {
        return .{
            .dir = dir,
            .segment_max_size = WAL_DEFAULT_SEGMENT_MAX,
            .payload_hash_type = record.PayloadHashType.Xxh64,
            .retention = RetentionParam.None,
            .buffer_size = 16384,
        };
    }
};
