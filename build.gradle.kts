import com.vanniktech.maven.publish.MavenPublishBaseExtension

plugins {
    alias(libs.plugins.maven.publish) apply false
    alias(libs.plugins.spring.boot) apply false
}

description = "Replicated, searchable ZDCache: a core library, a Spring Boot starter and a JSON cache service."

subprojects {
    plugins.withType<JavaPlugin> {
        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion = JavaLanguageVersion.of(21)
            }
        }

        tasks.withType<JavaCompile>().configureEach {
            options.release = 21
            options.encoding = "UTF-8"
            options.compilerArgs.add("-parameters")
        }

        tasks.withType<Javadoc>().configureEach {
            (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:all,-missing", "-quiet")
        }

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            testLogging {
                events("passed", "skipped", "failed")
            }
        }
    }

    plugins.withId("com.vanniktech.maven.publish") {
        extensions.configure<MavenPublishBaseExtension> {
            publishToMavenCentral()
            signAllPublications()
            pom {
                name = project.name
                description = provider { project.description }
                url = "https://github.com/zdcache/dzdcache"
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
                developers {
                    developer {
                        id = "steinard"
                        name = "Steinar Dragsnes"
                        email = "steinar.dragsnes@gmail.com"
                    }
                }
                scm {
                    connection = "scm:git:https://github.com/zdcache/dzdcache.git"
                    developerConnection = "scm:git:ssh://git@github.com/zdcache/dzdcache.git"
                    url = "https://github.com/zdcache/dzdcache"
                }
            }
        }
    }
}
