ARG JDK_VERSION=23

FROM eclipse-temurin:${JDK_VERSION}-jre
LABEL authors="BernardoFMF"

COPY target/eleceDb-*-jar-with-dependencies.jar /usr/app/eleceDb.jar
WORKDIR /usr/app

ENTRYPOINT ["java","-jar","eleceDb.jar"]