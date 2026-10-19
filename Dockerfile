FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY dzdcache-core ./dzdcache-core
COPY dzdcache-spring-boot-starter ./dzdcache-spring-boot-starter
COPY dzdcache-server ./dzdcache-server
RUN mvn -B -pl dzdcache-server -am package

# Runtime schema compilation needs jdk.compiler: a JRE image is insufficient.
FROM eclipse-temurin:21-jdk-jammy
WORKDIR /app
COPY --from=build /workspace/dzdcache-server/target/dzdcache-server.jar /app/dzdcache.jar
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-jar", "/app/dzdcache.jar"]
