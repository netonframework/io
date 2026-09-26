plugins { kotlin("multiplatform") version "2.4.0" }
kotlin {
    macosArm64 {
        // Declarations only: the symbols come from the libcrypto that the prebuilt provider links in,
        // so both sides of the comparison run the same OpenSSL build.
        compilations.getByName("main").cinterops.create("evp") { defFile(project.file("src/nativeInterop/cinterop/evp.def")) }
        binaries.executable { entryPoint = "main"; freeCompilerArgs += "-opt-in=kotlinx.cinterop.ExperimentalForeignApi" }
    }
    linuxX64 {
        compilations.getByName("main").cinterops.create("evp") { defFile(project.file("src/nativeInterop/cinterop/evp.def")) }
        binaries.executable { entryPoint = "main" }
    }
    sourceSets.getByName("nativeMain").dependencies {
        implementation("dev.whyoleg.cryptography:cryptography-core:0.6.0")
        implementation("dev.whyoleg.cryptography:cryptography-provider-openssl3-prebuilt:0.6.0")
    }
}
