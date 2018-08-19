import argparse
import json
import subprocess

from jinja2 import Template
import plotly.graph_objs as go
import psycopg2
from psycopg2.extras import DictCursor


def install_benchmark_database_objects():
    subprocess.call(["psql", "-f", "install_benchmark_database_objects.sql"])


def truncate_benchmark_results(cursor):
    cursor.execute("""
        TRUNCATE benchmark_results;
    """)


def render_html(figure, filename):
    with open('plotly_html_template.jinja', 'r') as template_file:
        template = Template(template_file.read())
        with open('{}.html'.format(filename), 'w') as outfile:
            outfile.write(template.render(figure=figure))


def generate_plotly(cursor, title, filename):
    cursor.execute("""
        WITH foo AS (
            SELECT
                tables,
                rows,
                EXTRACT(EPOCH FROM avg(duration)) AS duration
            FROM
                benchmark_results
            GROUP BY
                tables,
                rows
            ORDER BY
                tables,
                rows
        )
        SELECT
            rows,
            array_agg(tables order by tables) AS tables,
            array_agg(duration order by tables) AS durations
        FROM
            foo
        GROUP BY
            rows
        ORDER BY
            rows
    """)

    data = []

    for row in cursor.fetchall():
        trace = go.Scatter(x=row['tables'], y=row['durations'], name='{} rows'.format(row['rows']), legendgroup='whatever')

        data.extend([trace])

    layout = go.Layout(title=title, xaxis=dict(title='Tables'), yaxis=dict(title='Seconds', type='log', autorange=True))
    figure = go.Figure(data=data, layout=layout)

    render_html(json.dumps(figure.to_plotly_json()), filename)


def main():
    parser = argparse.ArgumentParser(description='Run a benchmark')
    parser.add_argument('filename', help='json input file')

    args = parser.parse_args()

    with open(args.filename) as f:
        benchmark_descriptions = json.load(f)

    install_benchmark_database_objects()

    connection = psycopg2.connect("", cursor_factory=DictCursor)
    connection.autocommit = True
    cursor = connection.cursor()

    for bd in benchmark_descriptions:
        truncate_benchmark_results(cursor)
        benchmarks = create_benchmarks(bd['max-tables'], bd['max-rows'], bd['max-id'], bd['extra-columns'], bd['create-indexes'], bd['output-filename'])
        run_benchmarks(benchmarks)
        generate_plotly(cursor, bd['plot-title'], bd['output-filename'])


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

        command = "\copy (SELECT * FROM benchmark_results WHERE rows = {}) TO benchmark_results/{}_{}_rows.csv DELIMITER ',' CSV HEADER;".format(benchmark['rows'], benchmark['output_filename'], benchmark['rows'])
        subprocess.call(["psql", "-c", command])


if __name__ == "__main__":
        main()
