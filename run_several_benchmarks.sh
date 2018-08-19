#!/usr/bin/env bash
export PGHOST="localhost"
export PGDATABASE="join_test"
export PGUSER="brian"

python run_benchmarks.py input.json
