FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app

COPY pom.xml .
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -DskipTests package dependency:copy-dependencies

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /app/target/coffer-1.0-SNAPSHOT.jar ./coffer.jar
COPY --from=build /app/target/dependency ./lib

ENTRYPOINT ["java", "-cp", "coffer.jar:lib/*", "io.github.brunomvpcosta.coffer.ClusterNode"]
