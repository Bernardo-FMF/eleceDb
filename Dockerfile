ARG JDK_VERSION=23

FROM openjdk:${JDK_VERSION}
LABEL authors="BernardoFMF"

COPY target/eleceDb-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/app/eleceDb.jar
WORKDIR /usr/app

ENTRYPOINT ["java","-jar","eleceDb.jar"]