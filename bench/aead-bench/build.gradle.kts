plugins { kotlin("multiplatform") version "2.4.0" }
kotlin {
    macosArm64 {
        // Declarations only: the symbols come from native-builds' libcrypto, the same one cryptography-kotlin's
        // prebuilt-nativebuilds provider links, so both sides run one OpenSSL build (verified: one 3.6.4 copy).
        compilations.getByName("main").cinterops.create("evp") { defFile(project.file("src/nativeInterop/cinterop/evp.def")) }
        binaries.executable { entryPoint = "main"; freeCompilerArgs += "-opt-in=kotlinx.cinterop.ExperimentalForeignApi" }
    }
    linuxX64 {
        compilations.getByName("main").cinterops.create("evp") { defFile(project.file("src/nativeInterop/cinterop/evp.def")) }
        binaries.executable { entryPoint = "main" }
    }
    sourceSets.getByName("nativeMain").dependencies {
        implementation("dev.whyoleg.cryptography:cryptography-core:0.6.0")
        implementation("dev.whyoleg.cryptography:cryptography-provider-openssl3-prebuilt-nativebuilds:0.6.0")
        implementation("com.ensody.nativebuilds:openssl-libcrypto:3.6.4")   // one libcrypto for both sides
    }
}
