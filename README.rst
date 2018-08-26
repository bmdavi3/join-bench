Benchmark Runner
----------------

Test the performance of joining a bunch of tables

Build
~~~~~

.. code::

    make build

Run
~~~

The database to target is specified by the standard postgres environment variables.  You'll also need to either set PGPASSWORD or configure a .pgpass file

.. code::

    PGHOST="localhost" PGDATABASE="join_test" PGUSER="brian" PGPASSWORD="pass" ./run_with_docker.sh input.json


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

Benchmark plots and CSV files are stored in the results/ directory by default
