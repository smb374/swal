const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const mod = b.addModule("swal", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    const lib = b.addLibrary(.{
        .name = "swal",
        .root_module = mod,
        .linkage = .dynamic,
    });
    b.installArtifact(lib);

    const lib_check = b.addLibrary(.{
        .name = "swal",
        .root_module = mod,
        .linkage = .dynamic,
    });
    const check = b.step("check", "Check if lib compiles");
    check.dependOn(&lib_check.step);

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
}
