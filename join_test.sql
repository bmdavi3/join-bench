\set num_tables 12
\set num_rows 1000




DROP FUNCTION IF EXISTS create_tables;
CREATE FUNCTION create_tables(num_tables integer, num_rows integer) RETURNS void AS $function_text$
BEGIN

DROP TABLE IF EXISTS table_1 CASCADE;
CREATE TABLE table_1 (
    id serial primary key
);

INSERT INTO table_1 (id)
SELECT
    nextval('table_1_id_seq')
FROM
    generate_series(1, num_rows);


FOR i IN 2..num_tables LOOP
    EXECUTE 'DROP TABLE IF EXISTS table_' || i || ' CASCADE;';

    EXECUTE format($$
        CREATE TABLE table_%1$s (
            id serial primary key,
            table_%2$s_id integer references table_%2$s (id)
	);

        INSERT INTO table_%1$s (table_%2$s_id)
        SELECT
            id
        FROM
            table_%2$s
        ORDER BY
            random();

        CREATE INDEX ON table_%1$s (table_%2$s_id);
        ANALYZE table_%1$s;
    $$, i, i-1);
END LOOP;
END;
$function_text$ LANGUAGE plpgsql;






DROP FUNCTION IF EXISTS get_query;
CREATE FUNCTION get_query(num_tables integer) RETURNS text AS $function_text$
DECLARE
    first_part text;
    second_part text;
    third_part text;
BEGIN

first_part := $query$
        SET search_path TO join_test;
        -- EXPLAIN ANALYZE
        SELECT
            count(*)
        FROM
            table_1 AS t1 INNER JOIN$query$;

second_part := '';

FOR i IN 2..num_tables-1 LOOP
    second_part := second_part || format($query$
            table_%1$s AS t%1$s ON
                t%2$s.id = t%1$s.table_%2$s_id INNER JOIN$query$, i, i-1);
END LOOP;

third_part := format($query$
            table_%1$s AS t%1$s ON
                t%2$s.id = t%1$s.table_%2$s_id$query$, num_tables, num_tables-1);

RETURN first_part || second_part || third_part;
END;
$function_text$ LANGUAGE plpgsql;



SELECT create_tables(:num_tables, :num_rows);
SELECT get_query(:num_tables);
