#!/usr/bin/env bash
docker pull akshayubhat/dva-auto:latest
docker-compose -f docker-compose.yml down -v
docker-compose -f docker-compose.yml up -d
