#!/usr/bin/env bash
IMAGE_NAME=${IMAGE_NAME:=yourimagerepo/vitex-stats-server}
IMAGE_VERSION=${IMAGE_VERSION:=1.12}

docker build -t vitex-stats-server .
docker tag vitex-stats-server:latest ${IMAGE_NAME}:${IMAGE_VERSION}
docker push ${IMAGE_NAME}:${IMAGE_VERSION}
