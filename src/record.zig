const std = @import("std");

pub const WAL_PCHK_XXH64: u8 = 0;
pub const WAL_PCHK_BLAKE2S: u8 = 1;

const Blake2s = std.crypto.hash.blake2.Blake2s(64);
const Crc32 = std.hash.crc.Crc32Iscsi;

pub const RecordError = error{
    InvalidHeader,
    InvalidPayload,
};

pub const PayloadHashType = enum(u8) {
    Xxh64 = WAL_PCHK_XXH64,
    Blake2s = WAL_PCHK_BLAKE2S,
};

pub const WALRecordHeader = extern struct {
    lsn: u64,
    prev_lsn: u64,
    resource_id: u64,
    txn_id: u64,
    record_len: u32,
    record_type: u16,
    payload_hash_type: u8,
    _pad: [5]u8,
    header_checksum: [4]u8,
    payload_checksum: [8]u8,

    const Self = @This();
    pub fn init(ifields: anytype) Self {
        return std.mem.zeroInit(Self, ifields);
    }

    pub fn to_bytes(self: *const Self) [56]u8 {
        var buf: [56]u8 = undefined;
        @memset(&buf, 0);
        self.encode(&buf) catch unreachable;

        return buf;
    }

    pub fn encode(self: *const Self, buf: []u8) !void {
        var writer = std.Io.Writer.fixed(buf);
        _ = try writer.writeStruct(self.*, .little);
    }

    pub fn write(self: *const Self, writer: *std.Io.Writer) !void {
        _ = try writer.writeStruct(self.*, .little);
    }

    pub fn decode(buf: []const u8) !WALRecordHeader {
        var reader = std.Io.Reader.fixed(buf);
        return try reader.takeStruct(WALRecordHeader, .little);
    }

    pub fn verify(self: *const WALRecordHeader) !void {
        const buf = self.to_bytes();
        const hhash = Crc32.hash(buf[0..44]);
        const shash = std.mem.readInt(u32, buf[44..48], .little);
        if (hhash != shash) {
            return error.InvalidHeader;
        }
    }

    pub fn update_checksum(self: *WALRecordHeader) void {
        const buf = self.to_bytes();
        const hhash = Crc32.hash(buf[0..44]);
        std.mem.writeInt(u32, &self.header_checksum, hhash, .little);
    }
};

pub const Record = struct {
    hdr: WALRecordHeader,
    payload: []const u8,
    own_payload: bool, // payload is owned

    pub fn init(hdr: WALRecordHeader, payload: []const u8) Record {
        var rec: Record = .{
            .hdr = hdr,
            .payload = payload,
            .own_payload = false,
        };
        rec.hdr.record_len = @truncate(payload.len + @sizeOf(WALRecordHeader));
        rec.hdr.update_checksum();
        switch (rec.hdr.payload_hash_type) {
            WAL_PCHK_BLAKE2S => {
                Blake2s.hash(payload, &rec.hdr.payload_checksum, .{});
            },
            else => {
                const hash = std.hash.XxHash64.hash(0, payload);
                std.mem.writeInt(u64, &rec.hdr.payload_checksum, hash, .little);
            },
        }

        return rec;
    }

    pub fn init_owned(hdr: WALRecordHeader, payloadd: []const u8, alloc: std.mem.Allocator) !Record {
        const obuf = try alloc.dupe(u8, payloadd);
        var rec = Record.init(hdr, obuf);
        rec.own_payload = true;
        return rec;
    }

    pub fn to_owned(self: *Record, alloc: std.mem.Allocator) !void {
        const obuf = try alloc.dupe(u8, self.payload);
        self.payload = obuf;
    }

    pub fn encode_size(self: *const Record) usize {
        return @sizeOf(WALRecordHeader) + self.payload.len;
    }

    pub fn encode(self: *Record, allocator: std.mem.Allocator) ![]u8 {
        const buf = try allocator.alloc(u8, self.encode_size());
        try self.hdr.encode(buf[0..@sizeOf(WALRecordHeader)]);
        @memcpy(buf[@sizeOf(WALRecordHeader)..], self.payload);
        return buf;
    }

    pub fn write(self: *const Record, writer: *std.Io.Writer) !void {
        try self.hdr.write(writer);
        _ = try writer.writeAll(self.payload);
    }

    pub fn decode(buf: []const u8) !Record {
        const hdr = try WALRecordHeader.decode(buf[0..@sizeOf(WALRecordHeader)]);
        const payload = buf[@sizeOf(WALRecordHeader)..];
        if (payload.len < @as(usize, @intCast(hdr.record_len)) - @sizeOf(WALRecordHeader)) {
            return error.NotEnoughInput;
        }
        return .{
            .hdr = hdr,
            .payload = payload,
            .own_payload = false,
        };
    }

    pub fn verify(self: *const Record) !void {
        try self.hdr.verify();
        if (self.hdr.record_len != @as(u32, @truncate(self.payload.len + @sizeOf(WALRecordHeader)))) {
            return error.InvalidPayload;
        }
        var hbuf: [8]u8 = undefined;
        @memset(&hbuf, 0);
        switch (self.hdr.payload_hash_type) {
            WAL_PCHK_BLAKE2S => {
                Blake2s.hash(self.payload, &hbuf, .{});
            },
            else => {
                const hash = std.hash.XxHash64.hash(0, self.payload);
                std.mem.writeInt(u64, &hbuf, hash, .little);
            },
        }
        if (!std.mem.eql(u8, &hbuf, &self.hdr.payload_checksum)) {
            return error.InvalidPayload;
        }
    }
};

const expect = std.testing.expect;

test "Record Header Size" {
    try expect(@sizeOf(WALRecordHeader) == 56);
}

test "Header encode" {
    var hdr: WALRecordHeader = WALRecordHeader.init(.{
        .lsn = 0xdeadbeef,
        .resource_id = 0xcafebeef,
        .record_len = 32,
    });
    hdr.update_checksum();
    const buf = hdr.to_bytes();
    try expect(std.mem.eql(u8, buf[44..48], &hdr.header_checksum));
}

test "Header decode" {
    var hdr: WALRecordHeader = WALRecordHeader.init(.{
        .lsn = 0xdeadbeef,
        .resource_id = 0xcafebeef,
        .record_len = 32,
    });
    hdr.update_checksum();
    const buf = hdr.to_bytes();
    const hdr2 = try WALRecordHeader.decode(&buf);
    try hdr2.verify();
    try expect(std.meta.eql(hdr, hdr2));
}

test "Header decode fail" {
    // Not enough size
    const buf1: [44]u8 = undefined;
    try expect(WALRecordHeader.decode(&buf1) == error.EndOfStream);
    // Checksum mismatch
    var hdr: WALRecordHeader = WALRecordHeader.init(.{
        .lsn = 0xdeadbeef,
        .resource_id = 0xcafebeef,
        .record_len = 32,
    });
    const buf = hdr.to_bytes();
    const hdr2 = try WALRecordHeader.decode(&buf);
    try expect(hdr2.verify() == error.InvalidHeader);
}

test "Create Record" {
    const data = "Hello World";
    const hdr: WALRecordHeader = WALRecordHeader.init(.{
        .lsn = 0xdeadbeef,
        .resource_id = 0xcafebeef,
    });
    const rec = Record.init(hdr, data[0..]);

    try rec.verify();
}

test "Blake2s checksum" {
    const data = "Hello World";
    const hdr: WALRecordHeader = WALRecordHeader.init(.{
        .lsn = 0xdeadbeef,
        .resource_id = 0xcafebeef,
        .payload_hash_type = WAL_PCHK_BLAKE2S,
    });
    const rec = Record.init(hdr, data[0..]);

    try rec.verify();
}

test "Record encode and decode" {
    const data = "Hello Zig";
    const hdr = WALRecordHeader.init(.{
        .lsn = 100,
        .resource_id = 200,
    });
    var rec = Record.init(hdr, data);

    const allocator = std.testing.allocator;
    const buf = try rec.encode(allocator);
    defer allocator.free(buf);

    const rec_decoded = try Record.decode(buf);
    try rec_decoded.verify();

    try expect(std.mem.eql(u8, rec.payload, rec_decoded.payload));
    try expect(std.meta.eql(rec.hdr, rec_decoded.hdr));
}

test "Record write" {
    const data = "Writing Data";
    const hdr = WALRecordHeader.init(.{ .lsn = 123 });
    var rec = Record.init(hdr, data);

    const buf = try std.testing.allocator.alloc(u8, 1024);
    defer std.testing.allocator.free(buf);

    var writer = std.Io.Writer.fixed(buf);
    try rec.write(&writer);

    const len = rec.encode_size();
    const rec_decoded = try Record.decode(buf[0..len]);
    try rec_decoded.verify();
    try expect(std.mem.eql(u8, data, rec_decoded.payload));
}

test "Record verification failures" {
    const data = "Integrity Check";
    const hdr = WALRecordHeader.init(.{ .lsn = 1 });
    var rec = Record.init(hdr, data);

    const allocator = std.testing.allocator;
    var buf = try rec.encode(allocator);
    defer allocator.free(buf);

    // 1. Corrupt payload checksum (modify last byte of payload)
    buf[buf.len - 1] ^= 0xFF;
    var rec_bad_payload = try Record.decode(buf);
    try expect(rec_bad_payload.verify() == error.InvalidPayload);
}

test "Record verify length mismatch" {
    const data = "Valid Data";
    const hdr = WALRecordHeader.init(.{ .lsn = 1 });
    var rec = Record.init(hdr, data);

    const allocator = std.testing.allocator;
    const buf = try rec.encode(allocator);
    defer allocator.free(buf);

    // Append extra byte to the buffer
    const large_buf = try allocator.alloc(u8, buf.len + 1);
    defer allocator.free(large_buf);
    @memcpy(large_buf[0..buf.len], buf);
    large_buf[buf.len] = 0; // Just add a dummy byte

    // Decode this larger buffer. The payload in rec_large will be longer than what hdr.record_len indicates.
    const rec_large = try Record.decode(large_buf);
    // Header is valid. Payload is 1 byte longer than expected.
    try expect(rec_large.verify() == error.InvalidPayload);
}
