plugins { kotlin("multiplatform") }
repositories { mavenCentral() }

// One artifact, like tokio is one crate: the byte buffer, the codec contracts, the I/O model and
// the reactor ship together. The packages (neton.io.bytes / codec / core / net) keep the layering
// readable; the artifact boundary does not need to.
//
// Targets are the ones the reactor runs on. Windows is out until the IOCP driver exists — a
// Windows klib carrying buffers and codecs but no I/O would be an I/O library in name only.
kotlin {
    val macos = listOf(macosArm64(), macosX64())
    val linux = listOf(linuxX64(), linuxArm64())
    // iOS client targets (device + simulator) reuse the appleMain kqueue reactor. No executable
    // entry points and no io_uring (Linux-only); the transport is a library here.
    iosArm64(); iosSimulatorArm64(); iosX64()

    (macos + linux).forEach { target ->
        target.binaries {
            // SPEC §19.2: IR inlining before codegen measured +3.2% on one core (9/12 paired rounds).
            executable("echoServer") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("preCodegenInlineThreshold", "40") }
            // Same server, stop-the-world mark&sweep (no GC thread): bench variable for SPEC §17c.
            executable("echoServerStw") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("gc", "stwms") }
            // Same server, no GC at all: bench-only upper bound for "what if allocation were free".
            executable("echoServerNoGc") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("gc", "noop") }
            // SPEC §19.2 single-variable variants of the same server (bench only).
            executable("echoServerPmcs") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("gc", "pmcs") }
            executable("echoServerMarkSt") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("gcMarkSingleThreaded", "true") }
            executable("echoServerInline40") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("preCodegenInlineThreshold", "40") }
            executable("echoClient") { entryPoint = "neton.io.net.echoClientMain" }
        }
    }

    // io_uring bindings (self-contained UAPI; the cross sysroot predates io_uring).
    linux.forEach { target ->
        target.compilations.getByName("main").cinterops.create("uring") {
            defFile(project.file("src/nativeInterop/cinterop/uring.def"))
        }
    }

    // SPEC §19.2: -Pneton.klibInliner=full turns on experimental cross-module IR inlining for every
    // compilation (A/B only; the default build does not set it).
    if (project.findProperty("neton.klibInliner") == "full") {
        targets.configureEach { compilations.configureEach { compileTaskProvider.configure {
            compilerOptions.freeCompilerArgs.add("-Xklib-ir-inliner=full")
        } } }
    }

    sourceSets {
        commonMain.dependencies {
            api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
        }
        commonTest.dependencies { implementation(kotlin("test")) }
    }
}
