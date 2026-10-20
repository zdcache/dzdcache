plugins {
    java
}

description = "Benchmarks dzdcache against other distributed caches; not published."

val benchmarkJava = javaToolchains.launcherFor {
    languageVersion = JavaLanguageVersion.of(providers.gradleProperty("benchmark.java").orElse("25").get().toInt())
}
val serverJar = project(":dzdcache-server").layout.buildDirectory.file("libs/dzdcache-server.jar").get().asFile.absolutePath
val reports = layout.buildDirectory.dir("reports/dzdcache-benchmark")
val plan = providers.gradleProperty("benchmark.plan").orElse("quick")
val planArgs = plan.map {
    when (it) {
        "full" -> listOf("-f", "3", "-wi", "1", "-w", "600ms", "-i", "2", "-r", "300ms")
        "quick" -> listOf("-f", "1", "-wi", "1", "-w", "600ms", "-i", "2", "-r", "300ms")
        else -> throw GradleException("benchmark.plan must be quick or full")
    }
}
val extraArgs = providers.gradleProperty("jmh.args").map { it.split(" ").filter(String::isNotBlank) }.orElse(listOf())
val subjects = providers.gradleProperty("benchmark.subjects").orElse("")

dependencies {
    implementation(platform(libs.spring.boot.dependencies))
    implementation(project(":dzdcache-spring-boot-starter"))
    implementation(libs.jmh.core)
    annotationProcessor(libs.jmh.generator)
    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testImplementation("org.assertj:assertj-core")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    dependsOn(":dzdcache-server:bootJar")
    systemProperty("dzdcache.server.jar", serverJar)
    maxHeapSize = "3g"
}

fun JavaExec.benchmarkJvm() {
    classpath = sourceSets.main.get().runtimeClasspath
    javaLauncher = benchmarkJava
    dependsOn(":dzdcache-server:bootJar")
    jvmArgs("-Ddzdcache.server.jar=$serverJar")
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
