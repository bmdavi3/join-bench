DROP FUNCTION IF EXISTS create_enums(integer, integer);
CREATE FUNCTION create_enums(num_tables integer, num_rows integer) RETURNS void AS $function_text$
DECLARE
    enum_label_text text := '';
BEGIN

SELECT string_agg($$'My Label #$$ || gs || $$'$$, ',') INTO enum_label_text FROM generate_series(1, num_rows) AS gs;

FOR i IN 1..num_tables LOOP
    EXECUTE 'DROP TYPE IF EXISTS enum_' || i || ' CASCADE;';
    EXECUTE 'CREATE TYPE enum_' || i || ' AS ENUM (' || enum_label_text || ');';
END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS create_enum_using_table(integer, integer, integer, integer);
CREATE FUNCTION create_enum_using_table(num_rows integer, num_enum_columns integer, num_enum_choices integer, extra_columns integer) RETURNS void AS $function_text$
DECLARE
    extra_column_text text := '';
    enum_column_text text := '';
    insert_text text;
BEGIN
    -- Extra column section
    FOR i IN 1..extra_columns LOOP
        extra_column_text := extra_column_text || ', extra_column_' || i || $$ varchar(20) default '12345678901234567890' $$;
    END LOOP;


    SELECT string_agg(', label_' || gs || ' enum_' || gs, ' ' ORDER BY gs) INTO enum_column_text FROM generate_series(1, num_enum_columns) AS gs;

    DROP TABLE IF EXISTS primary_table CASCADE;
    EXECUTE format($$
        CREATE TABLE primary_table (
            id serial primary key
            %1$s
            %2$s
        );
    $$, extra_column_text, enum_column_text);

    SELECT
        'INSERT INTO primary_table (' || string_agg('label_' || gs, ', ' ORDER BY gs) || ') VALUES (' || string_agg($$ ('My Label #' || (SELECT ceil((random() * $$ || num_enum_choices || '))::int))::enum_' || gs, ', ') || ');'
    INTO insert_text
    FROM
        generate_series(1, num_enum_columns) AS gs;

    EXECUTE insert_text;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS create_lookup_tables(integer, integer, integer);
CREATE FUNCTION create_lookup_tables(num_tables integer, num_rows integer, extra_columns integer) RETURNS void AS $function_text$
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

DROP FUNCTION IF EXISTS create_primary_table(integer, integer, integer);
CREATE FUNCTION create_primary_table(num_rows integer, num_lookup_tables integer, extra_columns integer) RETURNS void AS $function_text$
DECLARE
    extra_column_text text := '';
    foreign_key_text text;
    foreign_key_column_text text := '';
    foreign_key_insert_text text := '';
BEGIN
    -- Extra column section
    FOR i IN 1..extra_columns LOOP
        extra_column_text := extra_column_text || ', extra_column_' || i || $$ varchar(20) default '12345678901234567890' $$;
    END LOOP;

    -- Foreign key section
    SELECT
        string_agg(', table_' || gs || '_id integer references table_' || gs || '(id)', ' ')
    INTO foreign_key_text
    FROM
        generate_series(1, num_lookup_tables) AS gs;

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
    FOR i IN 1..num_lookup_tables LOOP
        foreign_key_column_text := foreign_key_column_text || ', table_' || i || '_id';
    END LOOP;

    -- Foreign key insert text section
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

DROP FUNCTION IF EXISTS analyze_tables(integer);
CREATE FUNCTION analyze_tables(num_tables integer) RETURNS void AS $function_text$
BEGIN

FOR i IN 1..num_tables LOOP
    EXECUTE 'ANALYZE table_' || i || ';';
END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_enum_query(integer, text);
CREATE FUNCTION get_enum_query(num_tables integer, label_equals text) RETURNS text AS $function_text$
DECLARE
    where_clause text := '';
    column_select_list text := '';
    column_equals_list text := '';
BEGIN
    FOR i IN 1..num_tables LOOP
        column_select_list := column_select_list || $$,
                label_$$ || i;
    END LOOP;

    IF label_equals IS NOT NULL THEN
        FOR i IN 1..num_tables LOOP
            column_equals_list := column_equals_list || $$
                label_$$ || i || $$ = '$$ || label_equals || $$'$$;
            IF i != num_tables THEN
                column_equals_list := column_equals_list || ' AND ';
            END IF;
        END LOOP;

        where_clause := $$
            WHERE$$ || column_equals_list;
    END IF;

    RETURN format($$
            SELECT
                id%1$s
            FROM
                primary_table %2$s;
    $$, column_select_list, where_clause);
END;
$function_text$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_query(integer, text);
CREATE FUNCTION get_query(num_tables integer, label_equals text) RETURNS text AS $function_text$
DECLARE
    where_clause text := '';
    join_list text := '';
    column_select_list text := '';
    column_equals_list text := '';
BEGIN
    FOR i IN 1..num_tables LOOP
        column_select_list := column_select_list || $$,
                t$$ || i || '.label as t' || i || '_label';
    END LOOP;

    FOR i IN 1..num_tables LOOP
        join_list := join_list || $$ INNER JOIN
                table_$$ || i || ' AS t' || i || $$ ON
                    t$$ || i || '.id = p.table_' || i || '_id';
    END LOOP;

    IF label_equals IS NOT NULL THEN
        FOR i IN 1..num_tables LOOP
            column_equals_list := column_equals_list || $$
                t$$ || i || $$.label = '$$ || label_equals || $$'$$;
            IF i != num_tables THEN
                column_equals_list := column_equals_list || ' AND ';
            END IF;
        END LOOP;

        where_clause := $$
            WHERE$$ || column_equals_list;
    END IF;

    RETURN format($$
            SELECT
                p.id%1$s
            FROM
                primary_table AS p%2$s %3$s;
    $$, column_select_list, join_list, where_clause);
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
