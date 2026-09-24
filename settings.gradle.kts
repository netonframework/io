pluginManagement {
    repositories { gradlePluginPortal(); mavenCentral(); google() }
}
rootProject.name = "neton-io-build"
// One module, one artifact: com.netonstream:neton-io. The root is named differently only so the
// module can carry the library's name (Gradle does not allow a subproject named like its root).
include(":neton-io")
