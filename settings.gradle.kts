pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "dzdcache"

include("dzdcache-core", "dzdcache-spring-boot-starter", "dzdcache-server", "dzdcache-benchmark")
