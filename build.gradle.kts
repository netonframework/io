plugins {
    kotlin("multiplatform") version "2.4.0" apply false
}
allprojects {
    group = "com.netonstream"
    version = "0.1.1-SNAPSHOT"
}

// ---------- Maven Central publishing ----------
//
// Same convention as Neton (see its RELEASING.md): publications are laid out, signed, under
// build/staging-repo by `publishAllPublicationsToStagingLocalRepository`, and that directory is
// zipped and uploaded as one Central Portal bundle. Kotlin/Native artifacts are klibs; a consumer
// must compile with the same Kotlin version as the publisher.
val unpublished = setOf<String>()
val pomDescriptions = mapOf(
    "io-testkit" to "com.netonstream:io-testkit - the IoStream conformance suite of com.netonstream:io, for stream implementations and wrappers",
    "io" to "com.netonstream:io (neton-io) - coroutine-native async I/O for Kotlin/Native: byte buffers, codec contracts, the IoStream/Filter/Framed model and a TCP reactor (kqueue on Apple, epoll/poll and io_uring on Linux) that doubles as the coroutine dispatcher; the I/O foundation of the Neton stack"
)

subprojects {
    if (name in unpublished) return@subprojects
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    afterEvaluate {
        val sub = this@subprojects
        val publishing = sub.extensions.getByType<org.gradle.api.publish.PublishingExtension>()
        val apiDocsJar = sub.tasks.register<Jar>("apiDocsJar") {
            archiveClassifier.set("javadoc")
            from(rootProject.file("README.md"))
        }

        // Only group / version / POM. The KMP plugin owns the artifactIds (one per target plus
        // the root metadata publication); overriding them would make the publications collide.
        publishing.publications.withType<MavenPublication>().configureEach {
            artifact(apiDocsJar)
            groupId = sub.group.toString()
            version = sub.version.toString()
            pom {
                name.set(sub.name)
                description.set(pomDescriptions[sub.name] ?: "neton-io - ${sub.name}")
                url.set("https://github.com/netonframework/io")
                licenses { license { name.set("Apache-2.0"); url.set("https://opensource.org/licenses/Apache-2.0") } }
                developers {
                    developer {
                        id.set("zoujiaqing")
                        name.set("zoujiaqing")
                        email.set("zoujiaqing@gmail.com")
                        organization.set("Neton Stream")
                        organizationUrl.set("https://netonstream.com")
                    }
                }
                scm {
                    url.set("https://github.com/netonframework/io")
                    connection.set("scm:git:git://github.com/netonframework/io.git")
                    developerConnection.set("scm:git:ssh://git@github.com/netonframework/io.git")
                }
            }
        }

        publishing.repositories {
            // Local file repository: the tree a Central Portal bundle is zipped from.
            maven {
                name = "stagingLocal"
                url = uri(rootProject.layout.buildDirectory.dir("staging-repo"))
            }
        }

        // Central rejects unsigned artifacts. In-memory key first (CI / no keyring), else keyring.
        val signing = sub.extensions.getByType<org.gradle.plugins.signing.SigningExtension>()
        val inMemoryKey = sub.findProperty("signingInMemoryKey") as String?
        when {
            !inMemoryKey.isNullOrBlank() -> {
                signing.useInMemoryPgpKeys(inMemoryKey, sub.findProperty("signingInMemoryKeyPassword") as String? ?: "")
                signing.sign(publishing.publications)
            }
            sub.hasProperty("signing.keyId") -> signing.sign(publishing.publications)
        }
        // KMP publications share sign tasks across targets; without this ordering Gradle reports
        // an implicit dependency between publishX and signY and fails the build.
        sub.tasks.withType<org.gradle.api.publish.maven.tasks.AbstractPublishToMaven>().configureEach {
            mustRunAfter(sub.tasks.withType<org.gradle.plugins.signing.Sign>())
        }
    }
}
