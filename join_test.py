import time
import psycopg2


def create_tables(cursor, num_tables):
    cursor.execute("""
        DROP TABLE IF EXISTS table_1 CASCADE;
        CREATE TABLE table_1 (
            id serial primary key
        );
    """)

    for table_num in xrange(2, num_tables + 1):
        cursor.execute("""
            DROP TABLE IF EXISTS table_{table_num} CASCADE;
            CREATE TABLE table_{table_num} (
                id serial primary key,
                table_{previous_table_num}_id integer references table_{previous_table_num} (id)
            );
        """.format(table_num=table_num, previous_table_num=table_num - 1))


def populate_tables(cursor, num_tables, num_rows, mod):
    for x in xrange(0, num_rows):
        cursor.execute("""
            INSERT INTO table_1 (id) VALUES (DEFAULT);
        """)

    for table_num in xrange(2, num_tables + 1):
        cursor.execute("""
            INSERT INTO table_{table_num} (table_{previous_table_num}_id)
            SELECT
                id
            FROM
                table_{previous_table_num}
            ORDER BY
                random();
            """.format(table_num=table_num, previous_table_num=table_num - 1))


def get_sql_statement(num_tables, max_id=None):
    query = """
        SET search_path TO join_test;
        -- EXPLAIN ANALYZE
        SELECT
            count(*)
        FROM
            table_1 AS t1 INNER JOIN"""

    for table_num in xrange(2, num_tables):
        query += """
            table_{table_num} AS t{table_num} ON
                t{previous_table_num}.id = t{table_num}.table_{previous_table_num}_id INNER JOIN""".format(table_num=table_num, previous_table_num=table_num - 1)

    query += """
            table_{max_table_num} AS t{max_table_num} ON
                t{penultimate_table_num}.id = t{max_table_num}.table_{penultimate_table_num}_id""".format(max_table_num=num_tables, penultimate_table_num=num_tables - 1)

    if max_id:
        query += """
        WHERE
            t1.id <= {max_id}""".format(max_id=max_id)

    return query + ';'


def create_indexes_on_foreign_keys(cursor, num_tables):
    for table_num in xrange(2, num_tables + 1):
        cursor.execute("""
            CREATE INDEX ON table_{table_num} (table_{previous_table_num}_id);
        """.format(table_num=table_num, previous_table_num=table_num - 1))


def analyze_tables(cursor, num_tables):
    for table_num in xrange(1, num_tables + 1):
        cursor.execute("""
            ANALYZE table_{table_num};
        """.format(table_num=table_num, previous_table_num=table_num - 1))


def drop_and_recreate_schema(cursor):
    """
    Drop all tables from any previous tests.  Important for measuring db size
    """

    cursor.execute("""
        DROP SCHEMA IF EXISTS join_test CASCADE;
        CREATE SCHEMA join_test;
    """)


def set_search_path(cursor):
    cursor.execute("""
        SET search_path TO join_test;
    """)


def get_db_size(cursor):
    cursor.execute("""
        SELECT
            pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname)) AS SIZE
        FROM
            pg_catalog.pg_database d
        WHERE
            d.datname = 'join_test'
    """)

    return cursor.fetchone()[0]


def main():
    conn = psycopg2.connect(host='localhost', dbname='join_test', user='bdavis')
    conn.set_session(autocommit=True)
    cursor = conn.cursor()

    num_tables = 50
    num_rows = 2560000



    cursor.execute('BEGIN;')

    drop_and_recreate_schema(cursor)
    set_search_path(cursor)
    create_tables(cursor, num_tables)
    populate_tables(cursor, num_tables, num_rows, 10)
    create_indexes_on_foreign_keys(cursor, num_tables)
    analyze_tables(cursor, num_tables)

    cursor.execute('COMMIT;')

    print "\nTables:          {}".format(num_tables)
    print "Rows per table:  {}\n".format(num_rows)

    print "DB Size:         {}\n".format(get_db_size(cursor))

    start_time = time.time()
    cursor.execute(get_sql_statement(num_tables))
    print "No WHERE clause: {}ms".format(int((time.time() - start_time) * 1000))

    start_time = time.time()
    cursor.execute(get_sql_statement(num_tables, 10))
    print "   WHERE clause: {}ms\n".format(int((time.time() - start_time) * 1000))


if __name__ == "__main__":
    main()
