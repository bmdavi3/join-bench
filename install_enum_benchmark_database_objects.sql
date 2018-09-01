DROP FUNCTION IF EXISTS create_primary_table(integer, integer, integer);
CREATE FUNCTION create_primary_table(num_rows integer, num_lookup_tables integer, extra_columns integer) RETURNS void AS $function_text$
DECLARE
    extra_column_text text;
    foreign_key_text text;
    foreign_key_column_text text;
    foreign_key_insert_text text;
BEGIN
    -- Extra column section
    extra_column_text := '';

    FOR i IN 1..extra_columns LOOP
        extra_column_text := extra_column_text || ', extra_column_' || i || $$ varchar(20) default '12345678901234567890' $$;
    END LOOP;


    -- Foreign key section
    foreign_key_text := '';

    IF num_lookup_tables > 0 THEN
        foreign_key_text := ', ';
    END IF;


    FOR i IN 1..num_lookup_tables LOOP
        foreign_key_text := foreign_key_text || 'table_' || i || '_id integer references table_' || i || '(id)';
        IF i != num_lookup_tables THEN
            foreign_key_text := foreign_key_text || ', ';
        END IF;
    END LOOP;


    -- Create primary table
    RAISE NOTICE 'Creating primary table...';
    DROP TABLE IF EXISTS primary_table CASCADE;
    EXECUTE format($$
        CREATE TABLE primary_table (
            id serial primary key
            %1$s
            %2$s
        );
    $$, extra_column_text, foreign_key_text);

    -- Foreign key column text section
    foreign_key_column_text := '';

    FOR i IN 1..num_lookup_tables LOOP
        foreign_key_column_text := foreign_key_column_text || ', table_' || i || '_id';
    END LOOP;

    -- Foreign key insert text section
    foreign_key_insert_text := '';

    FOR i IN 1..num_lookup_tables LOOP
        foreign_key_insert_text := foreign_key_insert_text || format($$
            , ceil((random() * (
                SELECT
                    max(id)
                FROM
                    table_%1$s
            )))::int
        $$, i);
    END LOOP;

    -- Insert primary table rows
    EXECUTE format($$
        INSERT INTO primary_table (id %1$s)
        SELECT
            nextval('table_1_id_seq')
            %2$s
        FROM
            generate_series(1, %3$s);
    $$, foreign_key_column_text, foreign_key_insert_text, num_rows);
END;
$function_text$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS create_lookup_tables(integer, integer, integer);
CREATE FUNCTION create_lookup_tables(num_tables integer, num_rows integer, extra_columns integer) RETURNS void AS $function_text$
DECLARE
    extra_column_text text;
BEGIN

extra_column_text := '';

IF extra_columns > 0 THEN
    extra_column_text := ', ';
END IF;


FOR i IN 1..extra_columns LOOP
    extra_column_text := extra_column_text || 'extra_column_' || i || $$ varchar(20) default '12345678901234567890' $$;
    IF i != extra_columns THEN
        extra_column_text := extra_column_text || ', ';
    END IF;
END LOOP;

FOR i IN 1..num_tables LOOP
    EXECUTE 'DROP TABLE IF EXISTS table_' || i || ' CASCADE;';

    RAISE NOTICE 'Creating and inserting into table...';

    EXECUTE format($$
        CREATE TABLE table_%1$s (
            id serial primary key
            %3$s ,
            label text not null
    	);

        INSERT INTO table_%1$s (label)
        SELECT
            'My Label #' || gs
        FROM
            generate_series(1, %2$s) AS gs;
    $$, i, num_rows, extra_column_text);

    RAISE NOTICE 'Done creating table';
END LOOP;
END;
$function_text$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS analyze_tables(integer);
CREATE FUNCTION analyze_tables(num_tables integer) RETURNS void AS $function_text$
BEGIN

FOR i IN 1..num_tables LOOP
    EXECUTE 'ANALYZE table_' || i || ';';
END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_query(integer, integer);
CREATE FUNCTION get_query(num_tables integer, max_id integer) RETURNS text AS $function_text$
DECLARE
    first_part text;
    second_part text;
    third_part text;
    where_clause text;
BEGIN

first_part := $query$
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
                t%2$s.id = t%1$s.table_%2$s_id
        WHERE
            t1.id <= %3$s$query$, num_tables, num_tables-1, max_id);

RETURN first_part || second_part || third_part || ';';
END;
$function_text$ LANGUAGE plpgsql;


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
