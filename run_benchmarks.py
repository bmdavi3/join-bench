import subprocess


"""
export PGHOST="localhost"
export PGDATABASE="join_test"
export PGUSER="brian"
"""


benchmark_descriptions = [
    {
        'max_tables': 50,
        'max_max_rows': 100000,
        'max_id': 10,
        'create_indexes': False,
    },
    {
        'max_tables': 50,
        'max_max_rows': 100000,
        'max_id': 10,
        'create_indexes': True,
    },
    {
        'max_tables': 200,
        'max_max_rows': 1000,
        'max_id': 10,
        'create_indexes': False,
    },
    {
        'max_tables': 200,
        'max_max_rows': 100000,
        'max_id': 10,
        'create_indexes': True,
    },
]


benchmarks = []


for bd in benchmark_descriptions:
    max_rows = 10

    while max_rows <= bd['max_max_rows']:
        benchmarks.append({
            'max_tables': bd['max_tables'],
            'rows': max_rows,
            'max_id':bd['max_id'],
            'create_indexes': bd['create_indexes']
        })

        max_rows = max_rows * 10


for benchmark in benchmarks:
    max_tables = "max_tables={}".format(benchmark['max_tables'])
    rows = "rows={}".format(benchmark['rows'])
    max_id = "max_id={}".format(benchmark['max_id'])
    create_indexes = "create_indexes={}".format(benchmark['create_indexes'])

    subprocess.call(["psql", "-v", max_tables, "-v", rows, "-v", max_id, "-v", create_indexes, "-f", "benchmark.sql"])

    command = "\copy benchmark_results TO /home/brian/angryjoin/benchmark_results/db.m4.large_max_tables_{}_rows_{}_max_id_{}_create_indexes_{}.csv DELIMITER ',' CSV HEADER;".format(benchmark['max_tables'], benchmark['rows'], benchmark['max_id'], benchmark['create_indexes'])
    subprocess.call(["psql", "-c", command])
