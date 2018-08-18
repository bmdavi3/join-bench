\i install_functions.sql

DROP TABLE IF EXISTS benchmark_results;
CREATE TABLE benchmark_results (
    tables integer NOT NULL,
    rows integer NOT NULL,
    extra_columns integer NOT NULL,
    max_id integer NOT NULL,
    create_indexes boolean,
    duration interval NOT NULL
);

DROP TYPE IF EXISTS benchmark CASCADE;
CREATE TYPE benchmark AS (
    tables integer,
    rows integer,
    extra_columns integer,
    max_id integer,
    create_indexes boolean,
    iterations integer
);

DROP FUNCTION IF EXISTS run_benchmarks(benchmark[], boolean);
CREATE FUNCTION run_benchmarks(benchmarks benchmark[], create_tables boolean) RETURNS void AS $function_text$
DECLARE
    benchmark benchmark;
    begin_time timestamptz;
    query_text text;
BEGIN


FOREACH benchmark IN ARRAY benchmarks LOOP
    IF create_tables THEN
        PERFORM create_tables(benchmark.tables, benchmark.rows, benchmark.create_indexes);
    END IF;

    SELECT get_query(benchmark.tables, benchmark.max_id) INTO query_text;
    RAISE NOTICE '%', query_text;

    FOR i IN 1..benchmark.iterations LOOP
        begin_time := clock_timestamp();
        EXECUTE query_text;

        INSERT INTO benchmark_results (tables, rows, extra_columns, max_id, create_indexes, duration)
        SELECT
            benchmark.tables,
            benchmark.rows,
	    benchmark.extra_columns,
	    benchmark.max_id,
            benchmark.create_indexes,
            clock_timestamp() - begin_time;
    END LOOP;
END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


SELECT create_tables(:'max_tables', :'rows', :'extra_columns', :'create_indexes');
SELECT analyze_tables(:'max_tables');



SELECT
    run_benchmarks(array_agg(ROW(s.a, :'rows', :'extra_columns', :'max_id', :'create_indexes', 10)::benchmark), False)
FROM
    generate_series(2, :'max_tables') AS s(a);


-- -- Display results
-- WITH results AS (
-- SELECT
--     *,
--     row_number() over (partition by tables, rows, max_id order by duration)
-- FROM
--     benchmark_results
-- )
-- SELECT
--     tables,
--     rows,
--     max_id,
--     avg(duration)
-- FROM
--     results
-- WHERE
--     row_number > 1
-- GROUP BY
--     tables,
--     rows,
--     max_id
-- ORDER BY
--     tables,
--     rows,
--     max_id;
