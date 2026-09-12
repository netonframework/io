pluginManagement {
    repositories { gradlePluginPortal(); mavenCentral(); google() }
}
rootProject.name = "neton-io"
include(":neton-io-bytes", ":neton-io-codec", ":neton-io-core", ":neton-io-testkit", ":neton-io-net")
