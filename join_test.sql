\set table_num 12






DROP FUNCTION IF EXISTS create_tables;
CREATE FUNCTION create_tables(integer) RETURNS void AS $function_text$
BEGIN

DROP TABLE IF EXISTS table_1 CASCADE;
CREATE TABLE table_1 (
    id serial primary key
);


FOR i IN 2..$1 LOOP
    EXECUTE 'DROP TABLE IF EXISTS table_' || i || ' CASCADE;';

    EXECUTE format($$
        CREATE TABLE table_%1$s (
            id serial primary key,
            table_%2$s_id integer references table_%2$s (id)
	);
    $$, i, i-1);

END LOOP;
END;
$function_text$ LANGUAGE plpgsql;


SELECT create_tables(:table_num);




    -- FOR r IN SELECT table_schema, table_name FROM information_schema.tables
    --              WHERE table_type = 'VIEW' AND table_schema = 'public'
    -- 		     LOOP
    -- 		             EXECUTE 'GRANT ALL ON ' || quote_ident(r.table_schema) || '.' || quote_ident(r.table_name) || ' TO webuser';
    -- 			         END LOOP;




