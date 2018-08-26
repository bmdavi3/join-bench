#!/usr/bin/env bash

docker run -v $PWD:/app --network host --env PGPASSWORD --env PGHOST --env PGDATABASE --env PGUSER -it --rm --name my-running-angryjoin angryjoin $1 "${@:2}"
