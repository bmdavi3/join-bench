#!/usr/bin/env bash
export PGHOST="localhost"
export PGDATABASE="join_test"
export PGUSER="brian"

python run_benchmarks.py --max-tables 50  --max-max-rows 100000 --max-id 10 --create-indexes False
python run_benchmarks.py --max-tables 50  --max-max-rows 100000 --max-id 10 --create-indexes True
python run_benchmarks.py --max-tables 200 --max-max-rows 1000   --max-id 10 --create-indexes False
python run_benchmarks.py --max-tables 200 --max-max-rows 100000 --max-id 10 --create-indexes True
