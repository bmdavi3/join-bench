import argparse
import subprocess


def main():
    parser = argparse.ArgumentParser(description='Run a benchmark')
    parser.add_argument('--max-tables', type=int, required=True)
    parser.add_argument('--max-max-rows', type=int, required=True)
    parser.add_argument('--max-id', type=int, required=True)
    parser.add_argument('--create-indexes', type=bool, required=True)

    args = parser.parse_args()

    benchmarks = create_benchmarks(args.max_tables, args.max_max_rows, args.max_id, args.create_indexes)
    run_benchmarks(benchmarks)


def create_benchmarks(max_tables, max_max_rows, max_id, create_indexes):
    benchmarks = []
    max_rows = 10

    while max_rows <= max_max_rows:
        benchmarks.append({
            'max_tables': max_tables,
            'rows': max_rows,
            'max_id': max_id,
            'create_indexes': create_indexes
        })

        max_rows = max_rows * 10

    return benchmarks


def run_benchmarks(benchmarks):
    for benchmark in benchmarks:
        max_tables = "max_tables={}".format(benchmark['max_tables'])
        rows = "rows={}".format(benchmark['rows'])
        max_id = "max_id={}".format(benchmark['max_id'])
        create_indexes = "create_indexes={}".format(benchmark['create_indexes'])
        extra_columns = "extra_columns={}".format(1)

        subprocess.call(["psql", "-v", max_tables, "-v", rows, "-v", max_id, "-v", create_indexes, "-v", extra_columns, "-f", "benchmark.sql"])

        command = "\copy benchmark_results TO benchmark_results/db.m4.large_max_tables_{}_rows_{}_max_id_{}_create_indexes_{}.csv DELIMITER ',' CSV HEADER;".format(benchmark['max_tables'], benchmark['rows'], benchmark['max_id'], benchmark['create_indexes'])
        subprocess.call(["psql", "-c", command])


if __name__ == "__main__":
        main()
