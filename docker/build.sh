docker buildx build --platform linux/amd64 --push -t repo.iris.tools/datafabric/openmetadata-service:v1.0.4 . -f docker/development/Dockerfile