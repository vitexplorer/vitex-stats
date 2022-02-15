#!/usr/bin/env bash

docker build -t vitex-stats-server .
docker tag vitex-stats-server:latest yourimagerepo/vitex-stats-server:1.2
docker push yourimagerepo/vitex-stats-server:1.2
