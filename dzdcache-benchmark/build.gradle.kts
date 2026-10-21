plugins {
    java
}

description = "Benchmarks dzdcache against other distributed caches; not published."

val benchmarkJava = javaToolchains.launcherFor {
    languageVersion = JavaLanguageVersion.of(providers.gradleProperty("benchmark.java").orElse("25").get().toInt())
}
val publishedImage = providers.gradleProperty("benchmark.serverImage").orElse("")
val serverImage = publishedImage.get().ifBlank { "dzdcache-server:benchmark" }
val repository = rootDir.absolutePath
val reports = layout.buildDirectory.dir("reports/dzdcache-benchmark")
val plan = providers.gradleProperty("benchmark.plan").orElse("quick")
val planArgs = plan.map {
    when (it) {
        "full" -> listOf("-f", "3", "-wi", "5", "-w", "3s", "-i", "3", "-r", "3s")
        "quick" -> listOf("-f", "1", "-wi", "3", "-w", "2s", "-i", "2", "-r", "1s")
        "custom" -> listOf()
        else -> throw GradleException("benchmark.plan must be quick, full or custom")
    }
}
val extraArgs = providers.gradleProperty("jmh.args").map { it.split(" ").filter(String::isNotBlank) }.orElse(listOf())
val igniteJvmArgs = listOf("java.base/jdk.internal.misc", "java.base/sun.nio.ch", "java.management/com.sun.jmx.mbeanserver",
    "jdk.internal.jvmstat/sun.jvmstat.monitor", "java.base/sun.reflect.generics.reflectiveObjects").map { "--add-exports=$it=ALL-UNNAMED" } +
    listOf("java.base/jdk.internal.access", "java.base/jdk.internal.misc", "java.base/sun.nio.ch", "java.base/sun.util.calendar",
        "java.management/com.sun.jmx.mbeanserver", "jdk.internal.jvmstat/sun.jvmstat.monitor", "java.base/sun.reflect.generics.reflectiveObjects",
        "jdk.management/com.sun.management.internal", "java.base/java.io", "java.base/java.nio", "java.base/java.net", "java.base/java.util",
        "java.base/java.util.concurrent", "java.base/java.util.concurrent.locks", "java.base/java.util.concurrent.atomic", "java.base/java.lang",
        "java.base/java.lang.invoke", "java.base/java.math", "java.sql/java.sql", "java.base/java.lang.reflect", "java.base/java.time",
        "java.base/java.text", "java.management/sun.management", "java.desktop/java.awt.font").map { "--add-opens=$it=ALL-UNNAMED" }
val subjects = providers.gradleProperty("benchmark.subjects").orElse("")
val persistence = providers.gradleProperty("benchmark.persistence").orElse("false")
val partitions = providers.gradleProperty("benchmark.partitions").orElse("1")

dependencies {
    implementation(platform(libs.spring.boot.dependencies))
    implementation(project(":dzdcache-spring-boot-starter"))
    implementation(libs.jmh.core)
    implementation(libs.hazelcast)
    implementation(enforcedPlatform(libs.infinispan.bom))
    implementation(libs.infinispan.api)
    implementation(libs.infinispan.core)
    implementation(libs.infinispan.query)
    compileOnly("org.infinispan:infinispan-component-annotations")
    annotationProcessor(libs.jmh.generator)
    implementation(libs.ignite.core)
    implementation(libs.ignite.indexing) { exclude(group = "org.apache.lucene", module = "lucene-analyzers-common") }
    implementation(libs.h2.ignite) { version { strictly(libs.versions.h2.ignite.get()) } }
    implementation(libs.jedis) { version { strictly(libs.versions.jedis.get()) } }
    implementation(libs.testcontainers)
    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testImplementation("org.assertj:assertj-core")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val buildServerImage by tasks.registering(Exec::class) {
    description = "Builds the dzdcache server image from this repository for the machine's own architecture."
    val usePublished = publishedImage.get().isNotBlank()
    onlyIf { !usePublished }
    commandLine("docker", "build", "-q", "-t", "dzdcache-server:benchmark", repository)
}

tasks.test {
    dependsOn(buildServerImage)
    systemProperty("dzdcache.server.image", serverImage)
    jvmArgs(igniteJvmArgs)
    maxHeapSize = "3g"
}

fun JavaExec.benchmarkJvm() {
    classpath = sourceSets.main.get().runtimeClasspath
    javaLauncher = benchmarkJava
    dependsOn(buildServerImage)
    jvmArgs("-Ddzdcache.server.image=$serverImage")
    jvmArgs("-Ddzdcache.benchmark.persistence=${persistence.get()}")
    jvmArgs("-Ddzdcache.benchmark.partitions=${partitions.get()}")
    jvmArgs(igniteJvmArgs)
    outputs.upToDateWhen { false }
    val dir = reports.get().asFile
    doFirst { dir.mkdirs() }
}

val jmh by tasks.registering(JavaExec::class) {
    description = "Runs the JMH comparison; choose the plan with -Pbenchmark.plan=quick|full and add options with -Pjmh.args=\"...\"."
    group = "verification"
    benchmarkJvm()
    mainClass = "org.openjdk.jmh.Main"
    val results = reports.get().file("results.json").asFile.absolutePath
    val chosen = subjects.get().takeIf(String::isNotBlank)?.let { listOf("-p", "subject=$it") } ?: listOf()
    val arguments = planArgs.get() + chosen + extraArgs.get() + listOf("-rf", "json", "-rff", results)
    args(arguments)
}

val freshReads by tasks.registering(JavaExec::class) {
    group = "verification"
    benchmarkJvm()
    mainClass = "io.github.dzdcache.benchmark.FreshReads"
    args(listOf(reports.get().file("fresh-reads.json").asFile.absolutePath) + listOfNotNull(subjects.get().takeIf(String::isNotBlank)))
}

val footprint by tasks.registering(JavaExec::class) {
    group = "verification"
    benchmarkJvm()
    maxHeapSize = "6g"
    mainClass = "io.github.dzdcache.benchmark.MemoryFootprint"
    args(listOf(reports.get().file("footprint.json").asFile.absolutePath) + listOfNotNull(subjects.get().takeIf(String::isNotBlank)))
}

val comparisonReport by tasks.registering(JavaExec::class) {
    group = "verification"
    benchmarkJvm()
    mainClass = "io.github.dzdcache.benchmark.ComparisonReport"
    mustRunAfter(jmh, freshReads, footprint)
    val dir = reports.get()
    args(listOf("results.json", "fresh-reads.json", "footprint.json", "report.md").map { dir.file(it).asFile.absolutePath } + plan.get())
}

tasks.register("compare") {
    description = "Runs the searches, key-value lookups, fresh reads and memory footprint, and writes report.md."
    group = "verification"
    dependsOn(jmh, freshReads, footprint, comparisonReport)
}

val writeBreakdown by tasks.registering(JavaExec::class) {
    description = "Times each step of a dzdcache write, embedded and in Docker, and writes write-breakdown.md."
    group = "verification"
    benchmarkJvm()
    mainClass = "io.github.dzdcache.benchmark.WriteBreakdown"
    jvmArgs(providers.gradleProperty("benchmark.jvmArgs").map { it.split(" ").filter(String::isNotBlank) }.orElse(listOf()).get())
    args(listOf(reports.get().file("write-breakdown.md").asFile.absolutePath, subjects.get(),
        providers.gradleProperty("benchmark.seconds").orElse("10").get(),
        providers.gradleProperty("benchmark.threads").orElse("4").get()))
}
