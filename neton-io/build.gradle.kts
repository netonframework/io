plugins { kotlin("multiplatform") }
repositories { mavenCentral() }

// One artifact, like tokio is one crate: the byte buffer, the codec contracts, the I/O model and
// the reactor ship together. The packages (neton.io.bytes / codec / core / net) keep the layering
// readable; the artifact boundary does not need to.
//
// Targets (SPEC §20, final): macOS, Linux, Windows, iOS, Android. neton-io is I/O and networking
// only (no TLS). Drivers: io_uring/epoll/poll on Linux, epoll/poll on Android, kqueue/poll on Apple,
// IOCP on Windows (WSAPoll first, for correctness).
kotlin {
    val linux = listOf(linuxX64(), linuxArm64())
    val macos = listOf(macosArm64(), macosX64())
    val windows = listOf(mingwX64())
    iosArm64(); iosSimulatorArm64(); iosX64()
    androidNativeArm64(); androidNativeArm32(); androidNativeX64(); androidNativeX86()

    // POSIX sockets are shared by Linux, Android and Apple; epoll by Linux and Android. io_uring
    // stays in linuxMain; Winsock in mingwMain.
    applyDefaultHierarchyTemplate {
        common {
            group("native") {
                group("posix") {
                    group("epoll") { group("linux"); group("androidNative") }
                    group("apple")
                }
            }
        }
    }

    // Desktop executables (bench servers/clients). Mobile targets ship the library only.
    (macos + linux + windows).forEach { target ->
        target.binaries {
            // SPEC §19.2: IR inlining before codegen measured +3.2% on one core (9/12 paired rounds).
            executable("echoServer") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("preCodegenInlineThreshold", "40") }
            executable("echoClient") { entryPoint = "neton.io.net.echoClientMain" }
            // SPEC §28.4 F2.
            executable("fairnessProbe") { entryPoint = "neton.io.net.fairnessProbeMain" }
        }
    }
    (macos + linux).forEach { target ->
        target.binaries {
            // Bench-only variants (SPEC §17c, §19.2).
            executable("echoServerStw") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("gc", "stwms") }
            executable("echoServerNoGc") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("gc", "noop") }
            executable("echoServerPmcs") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("gc", "pmcs") }
            executable("echoServerMarkSt") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("gcMarkSingleThreaded", "true") }
            executable("echoServerInline40") { entryPoint = "neton.io.net.echoServerMain"; binaryOption("preCodegenInlineThreshold", "40") }
        }
    }

    // Winsock helpers (integer socket options, WSAStartup) and ws2tcpip (getaddrinfo) for Windows.
    windows.forEach { target ->
        target.compilations.getByName("main").cinterops.create("winshim") {
            defFile(project.file("src/nativeInterop/cinterop/winshim.def"))
        }
    }

    // SPEC §27.1 / §27.2: signal-to-pipe handler and thread affinity helpers for every POSIX target.
    targets.withType<org.jetbrains.kotlin.gradle.plugin.mpp.KotlinNativeTarget>().matching { it.name != "mingwX64" }.configureEach {
        compilations.getByName("main").cinterops.create("posixshim") {
            defFile(project.file("src/nativeInterop/cinterop/posixshim.def"))
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
