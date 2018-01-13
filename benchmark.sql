\i install_functions.sql

DROP TABLE IF EXISTS benchmark_results;
CREATE TABLE benchmark_results (
    tables integer NOT NULL,
    rows integer NOT NULL,
    max_id integer,
    duration interval NOT NULL
);

DROP TYPE IF EXISTS benchmark CASCADE;
CREATE TYPE benchmark AS (
    tables integer,
    rows integer,
    max_id integer,
    iterations integer
);

DROP FUNCTION IF EXISTS run_benchmarks;
CREATE FUNCTION run_benchmarks(benchmarks benchmark[], create_tables boolean) RETURNS void AS $function_text$
DECLARE
    benchmark benchmark;
    begin_time timestamptz;
    query_text text;
BEGIN


FOREACH benchmark IN ARRAY benchmarks LOOP
    IF create_tables THEN
        PERFORM create_tables(benchmark.tables, benchmark.rows);
    END IF;

    SELECT get_query(benchmark.tables, benchmark.max_id) INTO query_text;
    RAISE NOTICE '%', query_text;

    FOR i IN 1..benchmark.iterations LOOP
        begin_time := clock_timestamp();
        EXECUTE query_text;

        INSERT INTO benchmark_results (tables, rows, max_id, duration)
        SELECT
            benchmark.tables,
            benchmark.rows,
            benchmark.max_id,
            clock_timestamp() - begin_time;
    END LOOP;
END LOOP;
END;
$function_text$ LANGUAGE plpgsql;

-- Needs server restart
-- set max_locks_per_transaction = 64000;

-- TODO: Improve benchmark logic to first generate max number of tables, and then generate 2-that many join queries, 10x each.
-- TODO: Take results and generate plots in "plotly"

\set max_tables 50
\set rows 10000

SELECT create_tables(:'max_tables', :'rows');
SELECT
    run_benchmarks(array_agg(ROW(s.a, :'rows', Null, 10)::benchmark), False)
FROM
    generate_series(2, :'max_tables') AS s(a);





-- Display results
WITH results AS (
SELECT
    *,
    row_number() over (partition by tables, rows, max_id order by duration)
FROM
    benchmark_results
)
SELECT
    tables,
    rows,
    max_id,
    avg(duration)
FROM
    results
WHERE
    row_number > 1
GROUP BY
    tables,
    rows,
    max_id
ORDER BY
    tables,
    rows,
    max_id
