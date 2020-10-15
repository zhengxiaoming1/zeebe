FROM maven:3-jdk-11 as builder
WORKDIR /usr/src/app
COPY src/ src/
COPY pom.xml pom.xml
RUN mvn clean package -DskipTests

FROM openjdk:11-jre as starter-gameday
USER nobody
CMD java $JAVA_OPTIONS -jar app.jar
COPY --from=builder /usr/src/app/target/starter.jar /app.jar

FROM openjdk:11-jre as worker-gameday
USER nobody
CMD java $JAVA_OPTIONS -jar app.jar
COPY --from=builder /usr/src/app/target/worker.jar /app.jar

FROM openjdk:11-jre as all-in-one-gameday
USER nobody
CMD java $JAVA_OPTIONS -jar app.jar
COPY --from=builder /usr/src/app/target/all-in-one.jar /app.jar
