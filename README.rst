Benchmark Runner
----------------

Test the performance of joining a bunch of tables

Usage
~~~~~

Edit run_several_benchmarks.sh to set PGHOST, PGDATABASE, PGUSER variables and list out the benchmarks to run, and then

.. code::

    PGHOST="localhost" PGDATABASE="join_test" PGUSER="brian" PGPASSWORD="pass" python run_benchmarks.py input.json


Input
~~~~~

Angryjoin takes a json file that describes the benchmarks to run.

.. code::

    $ cat input.json
    [
        {
            "max-tables": 20,
            "max-rows": 1000,
            "max-id": 10,
            "extra-columns": 0,
            "create-indexes": false,
            "output-filename": "benchmark_1",
            "plot-title": "My Plot Title 1"
        },
        {
            "max-tables": 30,
            "max-rows": 10000,
            "max-id": 10,
            "extra-columns": 5,
            "create-indexes": true,
            "output-filename": "benchmark_2,
            "plot-title": "My Plot Title 2"
        },
        ...
    ]


Output
~~~~~~

Benchmark results are stored as CSV files in the benchmark_results directory
