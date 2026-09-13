plugins { kotlin("multiplatform") }
repositories { mavenCentral() }
kotlin {
    val macos = listOf(macosArm64(), macosX64())
    val linux = listOf(linuxX64(), linuxArm64())
    // iOS client targets (simulator + device): reuse the appleMain kqueue reactor. No executable
    // entry points and no io_uring (Linux-only); the transport is a library here.
    iosArm64(); iosSimulatorArm64(); iosX64()

    (macos + linux).forEach { target ->
        target.binaries {
            executable("echoServer") { entryPoint = "neton.io.net.echoServerMain" }
            executable("echoClient") { entryPoint = "neton.io.net.echoClientMain" }
        }
    }

    // io_uring bindings (self-contained UAPI; the cross sysroot predates io_uring).
    linux.forEach { target ->
        target.compilations.getByName("main").cinterops.create("uring") {
            defFile(project.file("src/nativeInterop/cinterop/uring.def"))
        }
    }

    sourceSets {
        commonMain.dependencies {
            api(project(":neton-io-core"))
            api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
        }
        commonTest.dependencies { implementation(kotlin("test")) }
    }
}
