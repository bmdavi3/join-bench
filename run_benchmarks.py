import argparse
import json
import subprocess


"""
How are we going to describe benchmarks?

What can we specify?

  - max-tables
  - max-rows
  - max-id
  - whether we have indexes
  - how many extra columns
  - how many times to do each query
  - output file for results
"""


def main():
    parser = argparse.ArgumentParser(description='Run a benchmark')
    parser.add_argument('filename', help='json input file')

    args = parser.parse_args()

    with open(args.filename) as f:
        benchmark_descriptions = json.load(f)

    for bd in benchmark_descriptions:
        benchmarks = create_benchmarks(bd['max-tables'], bd['max-rows'], bd['max-id'], bd['extra-columns'], bd['create-indexes'], bd['output-filename'])
        run_benchmarks(benchmarks)


def create_benchmarks(max_tables, max_rows, max_id, extra_columns, create_indexes, output_filename):
    benchmarks = []
    rows = 10
    while rows <= max_rows:
        benchmarks.append({
            'max_tables': max_tables,
            'rows': rows,
            'max_id': max_id,
            'extra_columns': extra_columns,
            'create_indexes': create_indexes,
            'output_filename': output_filename,
        })

        rows = rows * 10

    return benchmarks


def run_benchmarks(benchmarks):
    for benchmark in benchmarks:
        max_tables = "max_tables={}".format(benchmark['max_tables'])
        rows = "rows={}".format(benchmark['rows'])
        max_id = "max_id={}".format(benchmark['max_id'])
        create_indexes = "create_indexes={}".format(benchmark['create_indexes'])
        extra_columns = "extra_columns={}".format(benchmark['extra_columns'])

        subprocess.call(["psql", "-v", max_tables, "-v", rows, "-v", max_id, "-v", create_indexes, "-v", extra_columns, "-f", "benchmark.sql"])

        command = "\copy benchmark_results TO benchmark_results/{} DELIMITER ',' CSV HEADER;".format(benchmark['output_filename'])
        subprocess.call(["psql", "-c", command])


if __name__ == "__main__":
        main()
