plugins { kotlin("multiplatform") }
repositories { mavenCentral() }
kotlin {
    macosArm64(); macosX64(); linuxX64(); linuxArm64(); mingwX64()
    iosArm64(); iosSimulatorArm64(); iosX64()
    sourceSets {
        commonTest.dependencies { implementation(kotlin("test")) }
    }
}
