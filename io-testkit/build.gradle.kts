plugins { kotlin("multiplatform") }
repositories { mavenCentral() }

// com.netonstream:io-testkit (SPEC §28.6): the IoStream conformance suite, published so protocol
// libraries and stream wrappers (TLS, ...) can run it against their own streams.
kotlin {
    linuxX64(); linuxArm64()
    macosArm64(); macosX64()
    mingwX64()
    iosArm64(); iosSimulatorArm64(); iosX64()
    androidNativeArm64(); androidNativeArm32(); androidNativeX64(); androidNativeX86()

    sourceSets {
        commonMain.dependencies {
            api(project(":io"))
            api(kotlin("test"))
        }
    }
}
