const std = @import("std");

pub fn build(b: *std.Build) void {
    var target = b.standardTargetOptions(.{});
    if (target.result.isGnuLibC()) target.result.abi = .musl;

    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addStaticLibrary(.{ .name = "lmdb-zig", .target = target, .optimize = optimize });
    const dep_lmdb_c = b.dependency("lmdb_c", .{ .target = target, .optimize = optimize });
    lib.addCSourceFiles(.{ .root = dep_lmdb_c.path("libraries/liblmdb"), .files = &.{ "mdb.c", "midl.c" }, .flags = &.{"-fno-sanitize=undefined"} });
    lib.linkLibC();
    b.installArtifact(lib);

    const mod = b.addModule("lmdb-zig-mod", .{ .root_source_file = b.path("lmdb.zig") });
    mod.addIncludePath(dep_lmdb_c.path(""));

    const tests = b.addTest(.{ .name = "test", .root_source_file = mod.root_source_file.?, .target = target, .optimize = optimize });
    tests.addIncludePath(b.path("lmdb/libraries/liblmdb"));
    tests.linkLibrary(lib);
    b.installArtifact(tests);
    const test_step = b.step("test", "Run libary tests");
    test_step.dependOn(&b.addRunArtifact(tests).step);

    const exe = b.addExecutable(.{ .name = "example", .root_source_file = b.path("example.zig"), .target = target, .optimize = optimize });
    exe.root_module.addImport("lmdb-zig", mod);
    exe.linkLibrary(lib);
    b.installArtifact(exe);
    const run_step = b.step("example", "Run example");
    run_step.dependOn(&b.addRunArtifact(exe).step);
}
