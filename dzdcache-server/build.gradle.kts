plugins {
    java
    alias(libs.plugins.spring.boot)
}

description = "Generic JSON cache service with runtime schema compilation and replication through an elected master."

dependencies {
    implementation(platform(libs.spring.boot.dependencies))
    implementation(project(":dzdcache-spring-boot-starter"))
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("io.micrometer:micrometer-registry-prometheus")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation(libs.jmh.core)
    testAnnotationProcessor(libs.jmh.generator)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.bootJar {
    archiveFileName = "dzdcache-server.jar"
}

tasks.jar {
    enabled = false
}

tasks.register<JavaExec>("jmh") {
    description = "Runs the in-process JMH benchmarks; pass JMH options with -Pjmh.args=\"...\"."
    group = "verification"
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "org.openjdk.jmh.Main"
    val results = layout.buildDirectory.file("reports/jmh/results.json")
    outputs.file(results)
    outputs.upToDateWhen { false }
    val extra = providers.gradleProperty("jmh.args").map { it.split(" ").filter(String::isNotBlank) }.orElse(listOf())
    argumentProviders.add(CommandLineArgumentProvider {
        extra.get() + listOf("-rf", "json", "-rff", results.get().asFile.absolutePath)
    })
    doFirst { results.get().asFile.parentFile.mkdirs() }
}
