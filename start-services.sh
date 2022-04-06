#!/bin/sh

set -e

docker run --rm -p 5432:5432 --name streampq-postgresql -d \
  -e POSTGRES_DB=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=password \
  postgres:14.2
