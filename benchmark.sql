\i install_functions.sql

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
