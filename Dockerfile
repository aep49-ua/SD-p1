FROM eclipse-temurin:17-jdk-alpine AS builder
WORKDIR /app

COPY . .


RUN javac -cp ".:lib/*" WM_Central.java

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

COPY --from=builder /app /app


EXPOSE 5000


ENV KAFKA_HOST=localhost:9092

CMD ["sh", "-c", "java -cp '.:lib/*' WM_Central 5000 ${KAFKA_HOST}"]
