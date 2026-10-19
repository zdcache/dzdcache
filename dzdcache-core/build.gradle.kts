plugins {
    `java-library`
    alias(libs.plugins.maven.publish)
}

description = "Replicated ZDCache engine, elections, log and discovery, without Spring."

dependencies {
    api(libs.zdcache)
    api(libs.jackson.databind)
    api(libs.micrometer.core)
    implementation(libs.slf4j.api)
    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
    testImplementation("ch.qos.logback:logback-classic")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.jar {
    manifest {
        attributes("Automatic-Module-Name" to "io.github.dzdcache.core")
    }
}
