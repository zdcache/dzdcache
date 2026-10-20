FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle ./gradle
COPY openapi.yaml ./
COPY dzdcache-core ./dzdcache-core
COPY dzdcache-spring-boot-starter ./dzdcache-spring-boot-starter
COPY dzdcache-server ./dzdcache-server
RUN ./gradlew --no-daemon :dzdcache-server:bootJar

# Runtime schema compilation needs jdk.compiler: a JRE image is insufficient.
FROM eclipse-temurin:21-jdk-jammy
WORKDIR /app
COPY --from=build /workspace/dzdcache-server/build/libs/dzdcache-server.jar /app/dzdcache.jar
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-jar", "/app/dzdcache.jar"]
