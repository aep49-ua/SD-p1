FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:17-jdk
WORKDIR /app
COPY --from=build /app/target/app.jar app.jar
RUN mkdir -p /data
ENV DB_PATH=/data/riego_central.db
ENV LANG=C.UTF-8
ENV JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8"
EXPOSE 5000 8080
ENTRYPOINT ["java", "-cp", "/app/app.jar", "WM_Central"]
CMD ["5000", "broker", "29092", "8080"]
