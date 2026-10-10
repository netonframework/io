plugins { kotlin("multiplatform") }
repositories { mavenCentral() }

// com.netonstream:io-testkit (SPEC §28.6): the IoStream conformance suite, published so protocol
// libraries and stream wrappers (TLS, ...) can run it against their own streams, native or JVM.
kotlin {
    // As neton-io's JVM target (its build explains the stdlib and API pins): the suite runs on its NIO reactor too.
    coreLibrariesVersion = "2.2.21"
    jvm {
        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_1_8)
            apiVersion.set(org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_2_2)
        }
    }
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
