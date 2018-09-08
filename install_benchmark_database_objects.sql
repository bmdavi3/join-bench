DROP FUNCTION IF EXISTS create_chained_tables(integer, integer, integer, boolean);
CREATE FUNCTION create_chained_tables(num_tables integer, num_rows integer, extra_columns integer, create_indexes boolean) RETURNS void AS $function_text$
DECLARE
    extra_column_text text;
BEGIN
    SELECT
        string_agg(', extra_column_' || gs || $$ varchar(20) default '12345678901234567890' $$, ' ')
    INTO extra_column_text
    FROM
        generate_series(1, extra_columns) AS gs;

    DROP TABLE IF EXISTS table_1 CASCADE;
    EXECUTE format($$
        CREATE TABLE table_1 (
            id serial primary key
            %1$s
        );
    $$, extra_column_text);

    INSERT INTO table_1 (id)
    SELECT
        nextval('table_1_id_seq')
    FROM
        generate_series(1, num_rows);

    FOR i IN 2..num_tables LOOP
        EXECUTE 'DROP TABLE IF EXISTS table_' || i || ' CASCADE;';

        RAISE NOTICE 'Creating and inserting into table...';

        EXECUTE format($$
            CREATE TABLE table_%1$s (
                id serial primary key
                %3$s ,
                table_%2$s_id integer references table_%2$s (id)
        );

            INSERT INTO table_%1$s (table_%2$s_id)
            SELECT
                id
            FROM
                table_%2$s
            ORDER BY
                random();
        $$, i, i-1, extra_column_text);

        IF create_indexes THEN
            RAISE NOTICE 'Creating index...';
            EXECUTE 'CREATE INDEX ON table_' || i || ' (table_' || i - 1 || '_id);';
        END IF;
        RAISE NOTICE 'Done creating table and index if necessary';
    END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_chained_query(integer, integer);
CREATE FUNCTION get_chained_query(num_tables integer, max_id integer) RETURNS text AS $function_text$
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


DROP FUNCTION IF EXISTS create_enums(integer, integer);
CREATE FUNCTION create_enums(enums_to_create integer, possible_values integer) RETURNS void AS $function_text$
DECLARE
    enum_label_text text := '';
BEGIN
    SELECT
        string_agg($$'My Label #$$ || gs || $$'$$, ',')
    INTO enum_label_text
    FROM
        generate_series(1, possible_values) AS gs;

    FOR i IN 1..enums_to_create LOOP
        EXECUTE 'DROP TYPE IF EXISTS enum_' || i || ' CASCADE;';
        EXECUTE 'CREATE TYPE enum_' || i || ' AS ENUM (' || enum_label_text || ');';
    END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS create_enum_using_table(integer, integer, integer, integer);
CREATE FUNCTION create_enum_using_table(num_rows integer, enum_columns integer, possible_values integer, extra_columns integer) RETURNS void AS $function_text$
DECLARE
    extra_column_text text;
    enum_column_text text := '';
    insert_text text;
BEGIN
    -- Extra column section
    SELECT
        string_agg(', extra_column_' || gs || $$ varchar(20) default '12345678901234567890' $$, ' ')
    INTO extra_column_text
    FROM
        generate_series(1, extra_columns) AS gs;


    SELECT string_agg(', label_' || gs || ' enum_' || gs, ' ' ORDER BY gs) INTO enum_column_text FROM generate_series(1, enum_columns) AS gs;

    DROP TABLE IF EXISTS primary_table CASCADE;
    EXECUTE format($$
        CREATE TABLE primary_table (
            id serial primary key
            %1$s
            %2$s
        );
    $$, extra_column_text, enum_column_text);

    SELECT
        'INSERT INTO primary_table (' || string_agg('label_' || gs, ', ' ORDER BY gs) || ') VALUES (' || string_agg($$ ('My Label #' || (SELECT ceil((random() * $$ || possible_values || '))::int))::enum_' || gs, ', ') || ');'
    INTO insert_text
    FROM
        generate_series(1, enum_columns) AS gs;

    EXECUTE insert_text;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_enum_query(integer, text);
CREATE FUNCTION get_enum_query(enum_columns integer, label_equals text) RETURNS text AS $function_text$
DECLARE
    where_clause text := '';
    column_select_list text := '';
BEGIN
    SELECT
        string_agg(',
                label_' || gs, '')
    INTO column_select_list
    FROM
        generate_series(1, enum_columns) AS gs;

    SELECT
        '
            WHERE
                ' || string_agg('label_' || gs || ' = ' || $$'$$ || label_equals || $$'$$, ' AND
                ')
    INTO where_clause
    FROM
        generate_series(1, enum_columns) AS gs
    WHERE
        label_equals IS NOT NULL;

    RETURN format($$
            SELECT
                id%1$s
            FROM
                primary_table %2$s;
    $$, column_select_list, where_clause);
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS create_fk_tables(integer, integer, integer);
CREATE FUNCTION create_fk_tables(num_tables integer, num_rows integer, extra_columns integer) RETURNS void AS $function_text$
DECLARE
    extra_column_text text;
BEGIN
    SELECT
        string_agg(', extra_column_' || gs || $$ varchar(20) default '12345678901234567890' $$, ' ')
    INTO extra_column_text
    FROM
        generate_series(1, extra_columns) AS gs;

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


DROP FUNCTION IF EXISTS create_fk_using_table(integer, integer, integer);
CREATE FUNCTION create_fk_using_table(num_rows integer, fk_columns integer, extra_columns integer) RETURNS void AS $function_text$
DECLARE
    extra_column_text text;
    foreign_key_text text;
    foreign_key_column_text text := '';
    foreign_key_insert_text text := '';
BEGIN
    -- Extra column section
    SELECT
        string_agg(', extra_column_' || gs || $$ varchar(20) default '12345678901234567890' $$, ' ')
    INTO extra_column_text
    FROM
        generate_series(1, extra_columns) AS gs;

    -- Foreign key section
    SELECT
        string_agg(', table_' || gs || '_id integer references table_' || gs || '(id)', ' ')
    INTO foreign_key_text
    FROM
        generate_series(1, fk_columns) AS gs;

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
    SELECT
        string_agg(', table_' || gs || '_id', ' ')
    INTO foreign_key_column_text
    FROM
        generate_series(1, fk_columns) AS gs;

    -- Foreign key insert text section
    FOR i IN 1..fk_columns LOOP
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


DROP FUNCTION IF EXISTS get_fk_query(integer, text);
CREATE FUNCTION get_fk_query(fk_columns integer, label_equals text) RETURNS text AS $function_text$
DECLARE
    where_clause text;
    join_list text;
    column_select_list text;
BEGIN
    SELECT
        string_agg($$,
                t$$ || gs || '.label AS t' || gs || '_label', '')
    INTO column_select_list
    FROM
        generate_series(1, fk_columns) AS gs;

    SELECT
        string_agg($$ INNER JOIN
                table_$$ || gs || ' AS t' || gs || $$ ON
                    t$$ || gs || '.id = p.table_' || gs || '_id', '')
    INTO join_list
    FROM
        generate_series(1, fk_columns) AS gs;

    SELECT
        $$
            WHERE
                $$ || string_agg('t' || gs || $$.label = '$$ || label_equals || $$'$$, $$ AND
                $$)
    INTO where_clause
    FROM
        generate_series(1, fk_columns) AS gs
    WHERE
        label_equals IS NOT NULL;

    RETURN format($$
            SELECT
                p.id%1$s
            FROM
                primary_table AS p%2$s %3$s;
    $$, column_select_list, join_list, where_clause);
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

        SELECT get_chained_query(benchmark.tables, benchmark.max_id) INTO query_text;
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
