Benchmark Runner
----------------

Test the performance of joining a bunch of tables

Usage
~~~~~

Edit run_several_benchmarks.sh to set PGHOST, PGDATABASE, PGUSER variables and list out the benchmarks to run, and then

.. code::

   make build
   PGHOST="localhost" PGDATABASE="join_test" PGUSER="brian" PGPASSWORD="pass" make run



Output
~~~~~~

Benchmark results are stored as CSV files in the benchmark_results directory
