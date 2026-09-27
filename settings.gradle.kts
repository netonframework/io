pluginManagement {
    repositories { gradlePluginPortal(); mavenCentral(); google() }
}
rootProject.name = "neton-io-build"
// One module, one artifact: com.netonstream:io (SPEC §28.13; com.netonstream:neton-io up to 0.1.0).
// The module is named for the artifact and lives in the neton-io/ directory; the root is named
// differently only because Gradle does not allow a subproject named like its root.
include(":io")
project(":io").projectDir = file("neton-io")
