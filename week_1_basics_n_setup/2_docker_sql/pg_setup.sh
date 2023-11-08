#!/usr/bin/env bash

# -e (env vars):  These are here to define "root" access
# -v (vol):       Bind mount volume
# -p (port):      Publish a container's port(s) to the host, mapping as <HOSTPORT>:<CONTAINERPORT>;
#                 map TCP port 5432 in the container to port 5431 on the Docker host, as 5432 taken

run_postgres () {
  docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5431:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13
}

docker_network () {
  docker network create pg-network
}

{
  docker_network && run_postgres
} ||
{
  run_postgres
}
 