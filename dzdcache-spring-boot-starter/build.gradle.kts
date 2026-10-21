plugins {
    `java-library`
    alias(libs.plugins.maven.publish)
}

description = "Embeds a replicated, searchable ZDCache in a Spring Boot service; its instances form a cluster."

dependencies {
    api(project(":dzdcache-core"))
    api(libs.spring.boot.autoconfigure)
    implementation(libs.spring.boot.starter.jackson)
    compileOnly(libs.spring.boot.health)
    compileOnly(platform(libs.spring.boot.dependencies))
    compileOnly("jakarta.servlet:jakarta.servlet-api")
    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation(libs.spring.boot.health)
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.jar {
    manifest {
        attributes("Automatic-Module-Name" to "io.github.dzdcache.spring")
    }
}
