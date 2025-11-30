const std = @import("std");
const record = @import("record.zig");
const queue = @import("queue.zig");
const ringbuf = @import("ringbuf.zig");
const utils = @import("utils.zig");
const atomic = std.atomic;

const Crc32 = std.hash.crc.Crc32Iscsi;
const WALConfig = @import("config.zig").WALConfig;

pub const WALRetentionMode = enum(u8) {
    None = 0,
    All = 1,
    LSN = 2,
    Count = 3,
};

pub const RetentionParam = union(WALRetentionMode) {
    None: void,
    All: void,
    LSN: u64,
    Count: u64,
};

pub const WAL_SEG_HEADER_OFF: comptime_int = 4096;
pub const WAL_SEG_MAGIC: u32 = 0x57534547;
pub const WAL_QUEUE_SIZE: usize = 8192;
pub const WAL_SWAP_THRES: usize = WAL_QUEUE_SIZE - 256;

pub const WALSegmentHeader = extern struct {
    magic: u32,
    version: u32,
    seg_id: u32,
    payload_hash_type: u8,
    record_alignment: u8,
    retention_mode: u8,
    _reserved: u8,
    min_lsn: u64,
    max_lsn: u64,
    size: u64,
    max_size: u64,
    retention_param: u64,
    buffer_size: u32,
    checksum: [4]u8,

    const Self = @This();
    const Size = @sizeOf(WALSegmentHeader);

    pub fn to_bytes(self: *const Self) [Size]u8 {
        var buf: [Size]u8 = undefined;
        @memset(&buf, 0);
        self.encode(&buf) catch unreachable;

        return buf;
    }

    pub fn encode(self: *const Self, buf: []u8) !void {
        var writer = std.Io.Writer.fixed(buf);
        try self.write(&writer);
    }

    pub fn write(self: *const Self, writer: *std.Io.Writer) !void {
        _ = try writer.writeStruct(self.*, .little);
    }

    pub fn decode(buf: []const u8) !WALSegmentHeader {
        var reader = std.Io.Reader.fixed(buf);
        return try reader.takeStruct(WALSegmentHeader, .little);
    }

    pub fn update_checksum(self: *Self) void {
        const bytes = self.to_bytes();
        const csum = Crc32.hash(&bytes);
        std.mem.writeInt(u32, &self.checksum, csum, .little);
    }
};

pub const WALCtx = struct {
    // Segment Management
    dir: std.fs.Dir,
    active_f: std.fs.File,
    active_seg: u32,
    active_shdr: *WALSegmentHeader,
    segment_max_size: u64,
    payload_hash_type: record.PayloadHashType,
    retention: RetentionParam,

    // Buffering
    epoch: atomic.Value(u64),
    active_writers: [2]atomic.Value(u32),
    queues: [2]*queue.MPSCQueue(WAL_QUEUE_SIZE),
    write_buf: std.Io.Writer, // Writer based on a fixed buffer

    // LSN
    next_lsn: atomic.Value(u64),
    flushed_lsn: atomic.Value(u64),
    checkpoint_lsn: atomic.Value(u64),
    pending_max_lsn: u64,

    // Locks
    flushing: atomic.Value(bool),
    mutex: std.Thread.Mutex,

    // Alloc
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(config: WALConfig, allocator: std.mem.Allocator) !*Self {
        var ctx: *WALCtx = try allocator.create(WALCtx);
        errdefer allocator.destroy(ctx);

        ctx.active_seg = 0;
        ctx.active_shdr = try allocator.create(WALSegmentHeader);
        errdefer allocator.destroy(ctx.active_shdr);
        ctx.segment_max_size = config.segment_max_size;
        ctx.payload_hash_type = config.payload_hsah_type;
        ctx.retention = config.retention;
        ctx.allocator = allocator;

        ctx.epoch = atomic.Value(u64).init(0);
        ctx.active_writers[0] = atomic.Value(u32).init(0);
        ctx.active_writers[1] = atomic.Value(u32).init(0);
        ctx.queues[0] = try queue.MPSCQueue(WAL_QUEUE_SIZE).init(allocator);
        errdefer ctx.queues[0].deinit();
        ctx.queues[1] = try queue.MPSCQueue(WAL_QUEUE_SIZE).init(allocator);
        errdefer ctx.queues[1].deinit();
        const buf = try allocator.alloc(u8, @as(usize, @intCast(config.buffer_size)));
        errdefer allocator.free(buf);
        ctx.write_buf = std.Io.Writer.fixed(buf);

        ctx.next_lsn = atomic.Value(u64).init(0);
        ctx.flushed_lsn = atomic.Value(u64).init(0);
        ctx.checkpoint_lsn = atomic.Value(u64).init(0);
        ctx.pending_max_lsn = 0;

        ctx.flushing = atomic.Value(bool).init(false);
        ctx.mutex = std.Thread.Mutex{};

        ctx.dir = try std.fs.cwd().openDir(config.dir, .{});
        errdefer ctx.dir.close();
        const f = try ctx.create_segment(0);
        ctx.active_f = f;
        ctx.active_shdr.* = ctx.default_seg_header();

        return ctx;
    }

    pub fn deinit(self: *Self) void {
        self.flush() catch {};
        self.flush() catch {};

        self.queues[0].deinit();
        self.queues[1].deinit();
        self.allocator.free(self.write_buf.buffer);
        self.active_f.close();
        self.dir.close();

        self.allocator.destroy(self);
    }

    // Use pointer instead of return for lsn for handling flush failure.
    pub fn append(self: *Self, rec: *record.Record, lsn_out: *u64) !void {
        const sz = rec.encode_size();
        std.debug.assert(sz < self.segment_max_size);
        if (sz < self.write_buf.buffer.len) {
            const idx = self.epoch.load(.acquire) % 2;
            {
                _ = self.active_writers[idx].fetchAdd(1, .release);
                defer _ = self.active_writers[idx].fetchSub(1, .release);

                const slot = try self.queues[idx].reserve();
                rec.hdr.lsn = self.next_lsn.fetchAdd(1, .acq_rel);
                lsn_out.* = rec.hdr.lsn;
                rec.hdr.update_checksum();
                self.queues[idx].produce(slot, rec) catch unreachable;
            }

            if (self.queues[idx].size.load(.acquire) > WAL_SWAP_THRES) {
                try self.flush();
            }
        } else {
            // Flush then write oversized record.
            try self.flush();
            self.mutex.lock();
            defer self.mutex.unlock();

            rec.hdr.lsn = self.next_lsn.fetchAdd(1, .acq_rel);
            lsn_out.* = rec.hdr.lsn;
            rec.hdr.update_checksum();
            // 512-byte alignedd write
            const psz = std.mem.alignForward(usize, sz, 512);
            const buf = try self.allocator.alloc(u8, psz);
            var writer = std.Io.Writer.fixed(buf);
            rec.write(&writer) catch unreachable;
            @memset(buf[sz..psz], 0);

            try self.active_f.pwriteAll(buf, self.active_shdr.size);
            self.active_shdr.size += sz;
            _ = self.flushed_lsn.fetchMax(lsn_out.*, .release);
            try self.sync_active_segment();

            if (rec.own_payload) {
                self.allocator.free(rec.payload);
            }
            self.allocator.destroy(rec);
        }
    }

    pub fn flush(self: *Self) !void {
        if (self.flushing.load(.acquire)) {
            return;
        }
        const epoch = self.epoch.load(.acquire);
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.epoch.cmpxchgStrong(epoch, epoch + 1, .release, .monotonic)) |_| {
            // Flush generation changed, giveup
            return;
        }

        self.flushing.store(true, .release);
        defer self.flushing.store(false, .release);

        const idx = epoch % 2;
        var max_lsn = self.flushed_lsn.load(.monotonic);

        while (true) {
            const need_rotate = try self.flush_step(idx, &max_lsn);
            _ = self.flushed_lsn.fetchMax(max_lsn, .release);
            if (need_rotate) {
                try self.rotate();
            }
            if (self.queues[idx].size.load(.acquire) > 0) continue;
            if (self.write_buf.end > 0) continue;
            if (self.active_writers[idx].load(.acquire) > 0) {
                var spins: u64 = 0;
                while (self.active_writers[idx].load(.acquire) > 0) : (spins += 1) {
                    utils.backoffHelper(spins);
                }
                continue;
            }
            break;
        }
        try self.sync_active_segment();
    }

    fn flush_step(self: *Self, idx: u64, max_lsn: *u64) !bool {
        var rec: ?*record.Record = null;
        var buffered_max_lsn: u64 = self.pending_max_lsn;

        while (self.queues[idx].consume()) |r| {
            rec = r;
            std.debug.assert(r.encode_size() <= self.write_buf.buffer.len);
            r.write(&self.write_buf) catch break;
            buffered_max_lsn = @max(buffered_max_lsn, r.hdr.lsn);
            if (r.own_payload) {
                self.allocator.free(r.payload);
            }
            self.allocator.destroy(r);
            rec = null;
        }

        const bsize = self.write_buf.end;
        if (bsize > 0) {
            const wbuf = self.write_buf.buffer;
            const wsize = std.mem.alignForward(usize, bsize, 512);
            @memset(wbuf[bsize..wsize], 0);

            try self.active_f.pwriteAll(wbuf[0..wsize], self.active_shdr.size);

            _ = self.write_buf.consumeAll();
            self.active_shdr.size += bsize;

            max_lsn.* = @max(max_lsn.*, buffered_max_lsn);
            self.pending_max_lsn = 0;
        }

        if (rec) |r| {
            r.write(&self.write_buf) catch unreachable;
            self.pending_max_lsn = @max(self.pending_max_lsn, r.hdr.lsn);
            if (r.own_payload) {
                self.allocator.free(r.payload);
            }
            self.allocator.destroy(r);
            rec = null;
        }
        // Need rotation or not.
        return self.active_shdr.size >= self.segment_max_size;
    }

    fn create_segment(self: *const Self, seg_id: u64) !std.fs.File {
        var buf: [std.posix.PATH_MAX]u8 = undefined;
        const sname = try std.fmt.bufPrint(&buf, "wal.{d:0>8}", .{seg_id});
        const new_f = try self.dir.createFile(sname, .{ .read = true });

        try std.posix.fsync(self.dir.fd);

        return new_f;
    }

    fn rotate(self: *Self) !void {
        try self.sync_active_segment();

        const cmax_lsn = self.flushed_lsn.load(.acquire);
        const nseg_id = self.active_seg + 1;
        const new_f = try self.create_segment(nseg_id);

        var nhdr = self.active_shdr.*;
        nhdr.seg_id = nseg_id;
        nhdr.min_lsn = cmax_lsn + 1;
        nhdr.max_lsn = 0;
        nhdr.size = WAL_SEG_HEADER_OFF;
        nhdr.update_checksum();

        const nbuf = nhdr.to_bytes();
        var page_buf: [WAL_SEG_HEADER_OFF]u8 = undefined;
        @memset(&page_buf, 0);
        @memcpy(page_buf[0..nbuf.len], &nbuf);

        try new_f.pwriteAll(&page_buf, 0);
        try new_f.sync();

        self.active_f.close();

        self.active_f = new_f;
        self.active_seg = nseg_id;
        self.active_shdr.* = nhdr;
    }

    fn sync_active_segment(self: *Self) !void {
        self.active_shdr.max_lsn = self.flushed_lsn.load(.acquire);
        self.active_shdr.update_checksum();
        const hdr_bytes = self.active_shdr.to_bytes();

        try self.active_f.pwriteAll(&hdr_bytes, 0);
        try self.active_f.sync();
    }

    fn default_seg_header(ctx: *const Self) WALSegmentHeader {
        var retention_param: u64 = 0;
        switch (ctx.retention) {
            .LSN => |cnt| {
                retention_param = cnt;
            },
            .Count => |cnt| {
                retention_param = cnt;
            },
            else => {},
        }
        return std.mem.zeroInit(WALSegmentHeader, .{
            .magic = WAL_SEG_MAGIC,
            .version = 1,
            .seg_id = ctx.active_seg,
            .payload_hash_type = @intFromEnum(ctx.payload_hash_type),
            .record_alignment = 8,
            .retention_mode = @intFromEnum(std.meta.activeTag(ctx.retention)),
            .min_lsn = ctx.flushed_lsn.raw,
            .size = WAL_SEG_HEADER_OFF,
            .max_size = ctx.segment_max_size,
            .retention_param = retention_param,
            .buffer_size = @as(u32, @truncate(ctx.write_buf.buffer.len)),
        });
    }
};

const expect = std.testing.expect;

test "Segment Header Size" {
    try expect(@sizeOf(WALSegmentHeader) <= 4096);
}
