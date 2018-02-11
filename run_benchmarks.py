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
        'max_id': 'Null',
    },
    {
        'max_tables': 50,
        'max_max_rows': 100000,
        'max_id': 10,
    },
    {
        'max_tables': 200,
        'max_max_rows': 1000,
        'max_id': 'Null',
    },
    {
        'max_tables': 200,
        'max_max_rows': 100000,
        'max_id': 10,
    },
]


benchmarks = []


for bd in benchmark_descriptions:
    max_rows = 10

    while max_rows <= bd['max_max_rows']:
        benchmarks.append({
            'max_tables': bd['max_tables'],
            'rows': max_rows,
            'max_id':bd['max_id']
        })

        max_rows = max_rows * 10


for benchmark in benchmarks:
    max_tables = "max_tables={}".format(benchmark['max_tables'])
    rows = "rows={}".format(benchmark['rows'])
    max_id = "max_id={}".format(benchmark['max_id'])

    subprocess.call(["psql", "-v", max_tables, "-v", rows, "-v", max_id, "-f", "benchmark.sql"])

    if benchmark['max_id'] == 'Null':
        max_id_text = ''
    else:
        max_id_text = '_max_id_{}'.format(benchmark['max_id'])

    command = "\copy benchmark_results TO /home/brian/angryjoin/benchmark_results/db.m4.large_max_tables_{}_rows_{}{}.csv DELIMITER ',' CSV HEADER;".format(benchmark['max_tables'], benchmark['rows'], max_id_text)
    subprocess.call(["psql", "-c", command])
