import argparse
import json
import os

from jinja2 import Template
import plotly.graph_objs as go
import psycopg2
from psycopg2.extras import DictCursor


def install_benchmark_database_objects(cursor):
    with open('install_benchmark_database_objects.sql') as objects_file:
        cursor.execute(objects_file.read())


def truncate_chained_benchmark_results(cursor):
    cursor.execute("""
        TRUNCATE chained_benchmark_results;
    """)


def truncate_enum_benchmark_results(cursor):
    cursor.execute("""
        TRUNCATE enum_benchmark_results;
    """)


def truncate_foreign_key_benchmark_results(cursor):
    cursor.execute("""
        TRUNCATE fk_benchmark_results;
    """)


def render_html(figure, filename):
    with open('plotly_html_template.jinja', 'r') as template_file:
        template = Template(template_file.read())
        with open('{}.html'.format(filename), 'w') as outfile:
            outfile.write(template.render(figure=figure))


def generate_chained_plotly(cursor, title, filename, output_dir):
    cursor.execute("""
        WITH foo AS (
            SELECT
                tables,
                rows,
                EXTRACT(EPOCH FROM avg(duration)) AS duration
            FROM
                chained_benchmark_results
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

    full_filename = os.path.join(output_dir, filename)

    render_html(json.dumps(figure.to_plotly_json()), full_filename)


def generate_enum_plotly(cursor, title, filename, output_dir):
    cursor.execute("""
        WITH foo AS (
            SELECT
                enums,
                rows,
                EXTRACT(EPOCH FROM avg(duration)) AS duration
            FROM
                enum_benchmark_results
            GROUP BY
                enums,
                rows
            ORDER BY
                enums,
                rows
        )
        SELECT
            rows,
            array_agg(enums order by enums) AS enums,
            array_agg(duration order by enums) AS durations
        FROM
            foo
        GROUP BY
            rows
        ORDER BY
            rows
    """)

    data = []

    for row in cursor.fetchall():
        trace = go.Scatter(x=row['enums'], y=row['durations'], name='{} rows'.format(row['rows']), legendgroup='whatever')

        data.extend([trace])

    layout = go.Layout(title=title, xaxis=dict(title='Enums'), yaxis=dict(title='Seconds', type='log', autorange=True))
    figure = go.Figure(data=data, layout=layout)

    full_filename = os.path.join(output_dir, filename)

    render_html(json.dumps(figure.to_plotly_json()), full_filename)


def generate_foreign_key_plotly(cursor, title, filename, output_dir):
    cursor.execute("""
        WITH foo AS (
            SELECT
                fk_tables,
                rows,
                EXTRACT(EPOCH FROM avg(duration)) AS duration
            FROM
                fk_benchmark_results
            GROUP BY
                fk_tables,
                rows
            ORDER BY
                fk_tables,
                rows
        )
        SELECT
            rows,
            array_agg(fk_tables order by fk_tables) AS fk_tables,
            array_agg(duration order by fk_tables) AS durations
        FROM
            foo
        GROUP BY
            rows
        ORDER BY
            rows
    """)

    data = []

    for row in cursor.fetchall():
        trace = go.Scatter(x=row['fk_tables'], y=row['durations'], name='{} rows'.format(row['rows']), legendgroup='whatever')

        data.extend([trace])

    layout = go.Layout(title=title, xaxis=dict(title='Fk_tables'), yaxis=dict(title='Seconds', type='log', autorange=True))
    figure = go.Figure(data=data, layout=layout)

    full_filename = os.path.join(output_dir, filename)

    render_html(json.dumps(figure.to_plotly_json()), full_filename)


def main():
    parser = argparse.ArgumentParser(description='Run a benchmark')
    parser.add_argument('filename', help='json input file')
    parser.add_argument('--output-dir', default='results', help='Directory to output results')

    args = parser.parse_args()

    with open(args.filename) as f:
        benchmark_descriptions = json.load(f)

    connection = psycopg2.connect("", cursor_factory=DictCursor)
    connection.autocommit = True
    cursor = connection.cursor()

    install_benchmark_database_objects(cursor)

    for bd in benchmark_descriptions:
        if bd['join-type'] == 'chained':
            truncate_chained_benchmark_results(cursor)
            benchmarks = create_chained_benchmarks(bd['max-tables'], bd['max-rows'], bd['max-id'], bd['extra-columns'],
                                                   bd['create-indexes'], bd['output-filename'])
            run_chained_benchmarks(cursor, benchmarks, args.output_dir)
            generate_chained_plotly(cursor, bd['plot-title'], bd['output-filename'], args.output_dir)
        elif bd['join-type'] == 'enums':
            truncate_enum_benchmark_results(cursor)
            benchmarks = create_enum_benchmarks(bd['max-rows'], bd['max-enums'], bd['possible-enum-values'],
                                                bd['extra-columns'], bd['where-clause'], bd['output-filename'])
            run_enum_benchmarks(cursor, benchmarks, args.output_dir)
            generate_enum_plotly(cursor, bd['plot-title'], bd['output-filename'], args.output_dir)
        elif bd['join-type'] == 'foreign-keys':
            truncate_foreign_key_benchmark_results(cursor)
            benchmarks = create_foreign_key_benchmarks(bd['max-primary-table-rows'], bd['max-fk-tables'], bd['fk-rows'],
                                                       bd['fk-extra-columns'], bd['extra-columns'], bd['where-clause'],
                                                       bd['output-filename'])
            run_foreign_key_benchmarks(cursor, benchmarks, args.output_dir)
            generate_foreign_key_plotly(cursor, bd['plot-title'], bd['output-filename'], args.output_dir)


def create_foreign_key_benchmarks(max_primary_table_rows, max_fk_tables, fk_rows, fk_extra_columns, extra_columns,
                                  where_clause, output_filename):
    benchmarks = []
    rows = 10
    while rows <= max_primary_table_rows:
        benchmarks.append({
            'rows': rows,
            'fk_tables': max_fk_tables,
            'fk_rows': fk_rows,
            'fk_extra_columns': fk_extra_columns,
            'extra_columns': extra_columns,
            'where_clause': where_clause,
            'output_filename': output_filename,
        })

        rows = rows * 10

    return benchmarks


def create_enum_benchmarks(max_rows, enums, possible_values, extra_columns, where_clause, output_filename):
    benchmarks = []
    rows = 10
    while rows <= max_rows:
        benchmarks.append({
            'rows': rows,
            'enums': enums,
            'possible_values': possible_values,
            'extra_columns': extra_columns,
            'where_clause': where_clause,
            'output_filename': output_filename,
        })

        rows = rows * 10

    return benchmarks


def create_chained_benchmarks(max_tables, max_rows, max_id, extra_columns, create_indexes, output_filename):
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


def execute_chained_benchmark(cursor, max_tables, rows, max_id, extra_columns, create_indexes):
    cursor.execute("""
        SELECT create_chained_tables(%(max_tables)s, %(rows)s, %(extra_columns)s, %(create_indexes)s);
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
            run_chained_benchmarks(array_agg(ROW(s.a, %(rows)s, %(extra_columns)s, %(max_id)s, %(create_indexes)s, 10)::chained_benchmark), False)
        FROM
            generate_series(2, %(max_tables)s) AS s(a);
    """, {
        'max_tables': max_tables,
        'rows': rows,
        'extra_columns': extra_columns,
        'create_indexes': create_indexes,
        'max_id': max_id,
    })


def execute_enum_benchmark(cursor, rows, enums, possible_values, extra_columns, where_clause):
    cursor.execute("""
        SELECT create_enums(%(enums)s, %(possible_values)s);
    """, {
        'enums': enums,
        'possible_values': possible_values,
    })

    cursor.execute("""
        SELECT create_enum_using_table(%(rows)s, %(enums)s, %(possible_values)s, %(extra_columns)s);
    """, {
        'rows': rows,
        'enums': enums,
        'possible_values': possible_values,
        'extra_columns': extra_columns,
    })

    cursor.execute("ANALYZE primary_table;")

    cursor.execute("""
        SELECT
            run_enum_benchmarks(array_agg(ROW(%(rows)s, s.a, %(possible_values)s, %(extra_columns)s, %(where_clause)s, 10)::enum_benchmark), False)
        FROM
            generate_series(1, %(enums)s) AS s(a);
    """, {
        'rows': rows,
        'enums': enums,
        'possible_values': possible_values,
        'extra_columns': extra_columns,
        'where_clause': where_clause,
    })


def execute_foreign_key_benchmark(cursor, rows, fk_tables, fk_rows, fk_extra_columns, extra_columns, where_clause):
    cursor.execute("""
        SELECT create_fk_tables(%(tables)s, %(rows)s, %(extra_columns)s);
    """, {
        'tables': fk_tables,
        'rows': fk_rows,
        'extra_columns': fk_extra_columns,
    })

    cursor.execute("""
        SELECT create_fk_using_table(%(rows)s, %(fk_tables)s, %(extra_columns)s);
    """, {
        'rows': rows,
        'fk_tables': fk_tables,
        'extra_columns': extra_columns,
    })

    cursor.execute("ANALYZE primary_table;")

    cursor.execute("""
        SELECT analyze_tables(%(tables)s);
    """, {
        'tables': fk_tables,
    })

    cursor.execute("""
        SELECT
            run_fk_benchmarks(array_agg(ROW(%(rows)s, s.a, %(fk_rows)s, %(fk_extra_columns)s, %(extra_columns)s, %(where_clause)s, 10)::fk_benchmark), False)
        FROM
            generate_series(1, %(fk_tables)s) AS s(a);
    """, {
        'rows': rows,
        'fk_tables': fk_tables,
        'fk_rows': fk_rows,
        'fk_extra_columns': fk_extra_columns,
        'extra_columns': extra_columns,
        'where_clause': where_clause,
    })


def run_foreign_key_benchmarks(cursor, benchmarks, output_dir):
    for benchmark in benchmarks:
        execute_foreign_key_benchmark(cursor, benchmark['rows'], benchmark['fk_tables'], benchmark['fk_rows'],
                                      benchmark['fk_extra_columns'], benchmark['extra_columns'], benchmark['where_clause'])

        filename = os.path.join(output_dir, '{}_{}_rows.csv'.format(benchmark['output_filename'], benchmark['rows']))

        with open(filename, 'w') as outfile:
            outfile.write("rows,fk_tables,fk_rows,fk_extra_columns,extra_columns,where_clause,duration\n")
            cursor.copy_to(outfile, """(SELECT rows, fk_tables, fk_rows, fk_extra_columns, extra_columns, where_clause, EXTRACT(EPOCH FROM duration) FROM fk_benchmark_results WHERE rows = {})""".format(benchmark['rows']), sep=',')


def run_enum_benchmarks(cursor, benchmarks, output_dir):
    for benchmark in benchmarks:
        execute_enum_benchmark(cursor, benchmark['rows'], benchmark['enums'], benchmark['possible_values'],
                               benchmark['extra_columns'], benchmark['where_clause'])

        filename = os.path.join(output_dir, '{}_{}_rows.csv'.format(benchmark['output_filename'], benchmark['rows']))

        with open(filename, 'w') as outfile:
            outfile.write("rows,enums,possible_values,extra_columns,where_clause,duration\n")
            cursor.copy_to(outfile, """(SELECT rows, enums, possible_values, extra_columns, where_clause, EXTRACT(EPOCH FROM duration) FROM enum_benchmark_results WHERE rows = {})""".format(benchmark['rows']), sep=',')


def run_chained_benchmarks(cursor, benchmarks, output_dir):
    for benchmark in benchmarks:
        execute_chained_benchmark(cursor, benchmark['max_tables'], benchmark['rows'], benchmark['max_id'],
                                  benchmark['extra_columns'], benchmark['create_indexes'])

        filename = os.path.join(output_dir, '{}_{}_rows.csv'.format(benchmark['output_filename'], benchmark['rows']))

        with open(filename, 'w') as outfile:
            outfile.write("tables,rows,extra_columns,max_id,create_indexes,duration\n")
            cursor.copy_to(outfile, """(SELECT tables, rows, extra_columns, max_id, create_indexes, EXTRACT(EPOCH FROM duration) FROM chained_benchmark_results WHERE rows = {})""".format(benchmark['rows']), sep=',')


if __name__ == "__main__":
        main()
