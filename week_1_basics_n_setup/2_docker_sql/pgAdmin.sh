#!/usr/bin/env bash

# -e (env vars):  These are here to define "root" access
# -p (port):      Publish a container's port(s) to the host, mapping as <HOSTPORT>:<CONTAINERPORT>;
#                 map TCP port 80 in the container to port 8080 on the Docker host

docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="admin" \
  -p 8080:80 \
  --network=pg-network \
  --name pg-admin \
  dpage/pgadmin4
