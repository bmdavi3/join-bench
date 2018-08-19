import argparse
import json

from jinja2 import Template
import plotly.graph_objs as go
import psycopg2
from psycopg2.extras import DictCursor


def install_benchmark_database_objects(cursor):
    with open('install_benchmark_database_objects.sql') as objects_file:
        cursor.execute(objects_file.read())


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

    connection = psycopg2.connect("", cursor_factory=DictCursor)
    connection.autocommit = True
    cursor = connection.cursor()

    install_benchmark_database_objects(cursor)

    for bd in benchmark_descriptions:
        truncate_benchmark_results(cursor)
        benchmarks = create_benchmarks(bd['max-tables'], bd['max-rows'], bd['max-id'], bd['extra-columns'], bd['create-indexes'], bd['output-filename'])
        run_benchmarks(cursor, benchmarks)
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


def execute_benchmark(cursor, max_tables, rows, max_id, extra_columns, create_indexes):
    cursor.execute("""
        SELECT create_tables(%(max_tables)s, %(rows)s, %(extra_columns)s, %(create_indexes)s);
    """, {
        'max_tables': max_tables,
        'rows': rows,
        'extra_columns': extra_columns,
        'create_indexes': create_indexes,
    })

    cursor.execute("""
        SELECT analyze_tables(%(max_tables)s);
    """, {
        'max_tables': max_tables,
    })

    cursor.execute("""
        SELECT
            run_benchmarks(array_agg(ROW(s.a, %(rows)s, %(extra_columns)s, %(max_id)s, %(create_indexes)s, 10)::benchmark), False)
        FROM
            generate_series(2, %(max_tables)s) AS s(a);
    """, {
        'max_tables': max_tables,
        'rows': rows,
        'extra_columns': extra_columns,
        'create_indexes': create_indexes,
        'max_id': max_id,
    })


def run_benchmarks(cursor, benchmarks):
    for benchmark in benchmarks:
        execute_benchmark(cursor, benchmark['max_tables'], benchmark['rows'], benchmark['max_id'], benchmark['extra_columns'], benchmark['create_indexes'])

        with open('/output/{}_{}_rows.csv'.format(benchmark['output_filename'], benchmark['rows']), 'w') as outfile:
            outfile.write("tables,rows,extra_columns,max_id,create_indexes,duration\n")
            cursor.copy_to(outfile, """(SELECT tables, rows, extra_columns, max_id, create_indexes, EXTRACT(EPOCH FROM duration) FROM benchmark_results WHERE rows = {})""".format(benchmark['rows']), sep=',')


if __name__ == "__main__":
        main()
