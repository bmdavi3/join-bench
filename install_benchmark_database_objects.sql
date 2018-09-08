DROP FUNCTION IF EXISTS create_chained_tables(integer, integer, integer, boolean);
CREATE FUNCTION create_chained_tables(tables integer, rows integer, extra_columns integer, create_indexes boolean) RETURNS void AS $function_text$
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
        generate_series(1, rows);

    FOR i IN 2..tables LOOP
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
CREATE FUNCTION get_chained_query(tables integer, max_id integer) RETURNS text AS $function_text$
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

    FOR i IN 2..tables-1 LOOP
        second_part := second_part || format($query$
                table_%1$s AS t%1$s ON
                    t%2$s.id = t%1$s.table_%2$s_id INNER JOIN$query$, i, i-1);
    END LOOP;

    third_part := format($query$
                table_%1$s AS t%1$s ON
                    t%2$s.id = t%1$s.table_%2$s_id
            WHERE
                t1.id <= %3$s$query$, tables, tables-1, max_id);

    RETURN first_part || second_part || third_part || ';';
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS create_enums(integer, integer);
CREATE FUNCTION create_enums(enums integer, possible_values integer) RETURNS void AS $function_text$
DECLARE
    enum_label_text text := '';
BEGIN
    SELECT
        string_agg($$'My Label #$$ || gs || $$'$$, ',')
    INTO enum_label_text
    FROM
        generate_series(1, possible_values) AS gs;

    FOR i IN 1..enums LOOP
        EXECUTE 'DROP TYPE IF EXISTS enum_' || i || ' CASCADE;';
        EXECUTE 'CREATE TYPE enum_' || i || ' AS ENUM (' || enum_label_text || ');';
    END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS create_enum_using_table(integer, integer, integer, integer);
CREATE FUNCTION create_enum_using_table(rows integer, enums integer, possible_values integer, extra_columns integer) RETURNS void AS $function_text$
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


    SELECT string_agg(', label_' || gs || ' enum_' || gs, ' ' ORDER BY gs) INTO enum_column_text FROM generate_series(1, enums) AS gs;

    DROP TABLE IF EXISTS primary_table CASCADE;
    EXECUTE format($$
        CREATE TABLE primary_table (
            id serial primary key
            %1$s
            %2$s
        );
    $$, extra_column_text, enum_column_text);

    -- TODO: Make sure we're inserting 'rows' number of rows
    SELECT
        'INSERT INTO primary_table (' || string_agg('label_' || gs, ', ' ORDER BY gs) || ') VALUES (' || string_agg($$ ('My Label #' || (SELECT ceil((random() * $$ || possible_values || '))::int))::enum_' || gs, ', ') || ');'
    INTO insert_text
    FROM
        generate_series(1, enums) AS gs;

    EXECUTE insert_text;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_enum_query(integer, text);
CREATE FUNCTION get_enum_query(enums integer, label_equals text) RETURNS text AS $function_text$
DECLARE
    where_clause text := '';
    column_select_list text := '';
BEGIN
    SELECT
        string_agg(',
                label_' || gs, '')
    INTO column_select_list
    FROM
        generate_series(1, enums) AS gs;

    SELECT
        '
            WHERE
                ' || string_agg('label_' || gs || ' = ' || $$'$$ || label_equals || $$'$$, ' AND
                ')
    INTO where_clause
    FROM
        generate_series(1, enums) AS gs
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
CREATE FUNCTION create_fk_tables(tables integer, rows integer, extra_columns integer) RETURNS void AS $function_text$
DECLARE
    extra_column_text text;
BEGIN
    SELECT
        string_agg(', extra_column_' || gs || $$ varchar(20) default '12345678901234567890' $$, ' ')
    INTO extra_column_text
    FROM
        generate_series(1, extra_columns) AS gs;

    FOR i IN 1..tables LOOP
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
        $$, i, rows, extra_column_text);

        RAISE NOTICE 'Done creating table';
    END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS create_fk_using_table(integer, integer, integer);
CREATE FUNCTION create_fk_using_table(rows integer, fk_tables integer, extra_columns integer) RETURNS void AS $function_text$
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
        generate_series(1, fk_tables) AS gs;

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
        generate_series(1, fk_tables) AS gs;

    -- Foreign key insert text section
    FOR i IN 1..fk_tables LOOP
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
    $$, foreign_key_column_text, foreign_key_insert_text, rows);
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_fk_query(integer, text);
CREATE FUNCTION get_fk_query(fk_tables integer, label_equals text) RETURNS text AS $function_text$
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
        generate_series(1, fk_tables) AS gs;

    SELECT
        string_agg($$ INNER JOIN
                table_$$ || gs || ' AS t' || gs || $$ ON
                    t$$ || gs || '.id = p.table_' || gs || '_id', '')
    INTO join_list
    FROM
        generate_series(1, fk_tables) AS gs;

    SELECT
        $$
            WHERE
                $$ || string_agg('t' || gs || $$.label = '$$ || label_equals || $$'$$, $$ AND
                $$)
    INTO where_clause
    FROM
        generate_series(1, fk_tables) AS gs
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
CREATE FUNCTION analyze_tables(tables integer) RETURNS void AS $function_text$
BEGIN
    FOR i IN 1..tables LOOP
        EXECUTE 'ANALYZE table_' || i || ';';
    END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


-- Chained
DROP TABLE IF EXISTS chained_benchmark_results;
CREATE TABLE chained_benchmark_results (
    tables integer NOT NULL,
    rows integer NOT NULL,
    extra_columns integer NOT NULL,
    max_id integer NOT NULL,
    create_indexes boolean,
    duration interval NOT NULL
);

DROP TYPE IF EXISTS chained_benchmark CASCADE;
CREATE TYPE chained_benchmark AS (
    tables integer,
    rows integer,
    extra_columns integer,
    max_id integer,
    create_indexes boolean,
    iterations integer
);


-- Foreign Key
DROP TABLE IF EXISTS fk_benchmark_results;
CREATE TABLE fk_benchmark_results (
    rows integer NOT NULL,
    fk_tables integer NOT NULL,
    fk_rows integer NOT NULL,
    fk_extra_columns integer,
    extra_columns integer NOT NULL,
    label_equals text,
    duration interval NOT NULL
);

DROP TYPE IF EXISTS fk_benchmark CASCADE;
CREATE TYPE fk_benchmark AS (
    rows integer NOT NULL,
    fk_tables integer NOT NULL,
    fk_rows integer NOT NULL,
    fk_extra_columns integer,
    extra_columns integer NOT NULL,
    label_equals text,
    iterations integer
);


-- Enum
DROP TABLE IF EXISTS enum_benchmark_results;
CREATE TABLE enum_benchmark_results (
    rows integer NOT NULL,
    enums integer NOT NULL,
    possible_values integer NOT NULL,
    extra_columns integer NOT NULL,
    label_equals text,
    duration interval NOT NULL
);

DROP TYPE IF EXISTS enum_benchmark CASCADE;
CREATE TYPE enum_benchmark AS (
    rows integer,
    enums integer,
    possible_values integer NOT NULL,
    extra_columns integer,
    label_equals text,
    iterations integer
);


DROP FUNCTION IF EXISTS run_chained_benchmarks(chained_benchmark[], boolean);
CREATE FUNCTION run_chained_benchmarks(chained_benchmarks benchmark[], create_tables boolean) RETURNS void AS $function_text$
DECLARE
    benchmark chained_benchmark;
    begin_time timestamptz;
    query_text text;
BEGIN
    FOREACH benchmark IN ARRAY benchmarks LOOP
        IF create_tables THEN
            PERFORM create_chained_tables(benchmark.tables, benchmark.rows, benchmark.create_indexes);
        END IF;

        SELECT get_chained_query(benchmark.tables, benchmark.max_id) INTO query_text;
        RAISE NOTICE '%', query_text;

        FOR i IN 1..benchmark.iterations LOOP
            begin_time := clock_timestamp();
            EXECUTE query_text;

            INSERT INTO chained_benchmark_results (tables, rows, extra_columns, max_id, create_indexes, duration)
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


DROP FUNCTION IF EXISTS run_fk_benchmarks(fk_benchmark[], boolean);
CREATE FUNCTION run_fk_benchmarks(benchmarks fk_benchmark[], create_tables boolean) RETURNS void AS $function_text$
DECLARE
    benchmark fk_benchmark;
    begin_time timestamptz;
    query_text text;
BEGIN
    FOREACH benchmark IN ARRAY benchmarks LOOP
        IF create_tables THEN
            PERFORM create_fk_tables(benchmark.fk_tables, benchmark.fk_rows, benchmark.fk_extra_columns);
            PERFORM create_fk_using_table(benchmark.rows, benchmark.fk_tables, benchmark.extra_columns);
        END IF;

        SELECT get_fk_query(benchmark.fk_tables, benchmark.label_equals) INTO query_text;
        RAISE NOTICE '%', query_text;

        FOR i IN 1..benchmark.iterations LOOP
            begin_time := clock_timestamp();
            EXECUTE query_text;

            INSERT INTO fk_benchmark_results (rows, fk_tables, fk_rows, fk_extra_columns, extra_columns, label_equals, duration)
            SELECT
                benchmark.rows,
                benchmark.fk_tables,
                benchmark.fk_rows,
                benchmark.fk_extra_columns,
                benchmark.extra_columns,
                benchmark.label_equals,
                clock_timestamp() - begin_time;
        END LOOP;
    END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS run_enum_benchmarks(enum_benchmark[], boolean);
CREATE FUNCTION run_enum_benchmarks(benchmarks enum_benchmark[], create_tables boolean) RETURNS void AS $function_text$
DECLARE
    benchmark enum_benchmark;
    begin_time timestamptz;
    query_text text;
BEGIN
    FOREACH benchmark IN ARRAY benchmarks LOOP
        IF create_tables THEN
            PERFORM create_enums(benchmark.enums, benchmark.possible_values);
            PERFORM create_enum_using_table(benchmark.rows, benchmark.enums, benchmark.possible_values, benchmark.extra_columns);
        END IF;

        SELECT get_enum_query(benchmark.enums, benchmark.label_equals) INTO query_text;
        RAISE NOTICE '%', query_text;

        FOR i IN 1..benchmark.iterations LOOP
            begin_time := clock_timestamp();
            EXECUTE query_text;

            INSERT INTO enum_benchmark_results (rows, enums, possible_values, extra_columns, label_equals, duration)
            SELECT
                benchmark.rows,
                benchmark.enums,
                benchmark.possible_values,
                benchmark.extra_columns,
                benchmark.label_equals,
                clock_timestamp() - begin_time;
        END LOOP;
    END LOOP;
END;
$function_text$ LANGUAGE plpgsql;
