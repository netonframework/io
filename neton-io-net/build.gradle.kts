plugins { kotlin("multiplatform") }
repositories { mavenCentral() }
kotlin {
    val nativeTargets = listOf(macosArm64(), macosX64(), linuxX64(), linuxArm64())
    nativeTargets.forEach { target ->
        target.binaries {
            executable("echoServer") { entryPoint = "neton.io.net.echoServerMain" }
            executable("echoClient") { entryPoint = "neton.io.net.echoClientMain" }
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
