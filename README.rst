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
            "join-type": "chained",
            "max-tables": 10,  # Queries will start by joining 2 tables, increasing by one until all tables are joined.  Number of tables joined will be the X axis on the plot.
            "max-rows": 10000,  # Benchmarks will be performed at 10 rows, 100 rows, etc. until max-rows is reached.  Creating a separate line on the plot for each.
            "extra_columns": 2,
            "max_id": 5,
            "create-indexes": true,
            "output-filename": "benchmark_1",
            "plot-title": "My Chained Benchmark Title"
        },
        {
            "join-type": "enums",
            "max-rows": 10000,  # Benchmarks will be performed at 10 rows in the primary table, increasing by a factor of 10 until max-rows is reached
            "max-enums": 100,  # Queries will start by selecting (and optionally filtering by) 1 enum column, increasing by one until max-enums is reached
            "possible-enum-values": 10,
            "extra-columns": 2,
            "where-clause": true,
            "output-filename": "benchmark_1",
            "plot-title": "My Enum Benchmark Title"
        },
        {
            "join-type": "foreign-keys",
            "max-primary-table-rows": 10000,  # Benchmarks will be performed at 10 rows in the primary table, increasing by a factor of 10 until max-rows is reached
            "max-fk-tables": 100,  # Queries will start by selecting from (and optionally filtering by) 1 foreign key table, increasing by one until max-fk-tables is reached
            "fk-rows": 100,
            "fk-extra-columns": 2,
            "extra-columns": 2,
            "where-clause": true,
            "output-filename": "benchmark_1",
            "plot-title": "My Foreign Key Benchmark Title"
        }
    ]



Output
~~~~~~

Benchmark plots and CSV files are stored in the results/ directory by default
