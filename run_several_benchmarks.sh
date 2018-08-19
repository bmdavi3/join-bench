#!/usr/bin/env bash
export PGHOST="localhost"
export PGDATABASE="join_test"
export PGUSER="brian"

docker run -v /home/brian/angryjoin:/input -v /home/brian/angryjoin/benchmark_results:/output --network host --env PGPASSWORD=pass --env PGHOST=localhost --env PGDATABASE=join_test --env PGUSER=brian -it --rm --name my-running-angryjoin angryjoin
