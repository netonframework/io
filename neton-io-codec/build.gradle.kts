plugins { kotlin("multiplatform") }
repositories { mavenCentral() }
kotlin {
    macosArm64(); macosX64(); linuxX64(); linuxArm64(); mingwX64()
    sourceSets {
        commonMain.dependencies { api(project(":neton-io-bytes")) }
        commonTest.dependencies { implementation(kotlin("test")) }
    }
}
