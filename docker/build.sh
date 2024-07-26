#!/bin/bash

#~/.m2/wrapper/dists/apache-maven-3.8.6-bin/1ks0nkde5v1pk9vtc31i9d0lcd/apache-maven-3.8.6/bin/mvn clean install -DskipTests
docker buildx build --platform linux/amd64 --push -t repo.iris.tools/datafabric/openmetadata-service:v1.0.16 . -f docker/development/Dockerfile