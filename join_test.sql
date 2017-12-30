\set num_tables 12
\set num_rows 10000


/*
What’s in a Join?

When designing database schemas for a new feature, and we’re considering whether to normalize something, people often ask “Is it worth another join?”

Some argue for normalization and say yes, it’s worth it. Others see the extra join that will be necessary and push back, worrying about the performance overhead of another join.
I’ve spent countless hours hashing this same conversation out over and over again, almost always arguing for normalization, but not I or anyone else ever had any numbers to back up their claim.
So let’s take a look and see.
*/



/*
  Since we'll be creating a varying number of tables, and sometimes a lot of them, let's create a function to make them for us.  Each table will reference the table before it, and we'll specify the number of tables it should create and how many rows should be in each.
*/


DROP FUNCTION IF EXISTS create_tables;
CREATE FUNCTION create_tables(num_tables integer, num_rows integer) RETURNS void AS $function_text$
BEGIN

-- There's no table before the first one, so this one's a little different.  Create it here instead of in our loop.
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

/*
  So let's give it a whirl and check out a couple tables it generates
*/

SELECT create_tables(10, 10000);

SELECT * from table_1 limit 10;

/*
 id 
----
  1
  2
  3
  4
  5
  6
  7
  8
  9
 10
(10 rows)
*/

SELECT * from table_2 limit 10;

/*
 id | table_1_id 
----+------------
  1 |        824
  2 |        973
  3 |        859
  4 |        789
  5 |        901
  6 |        112
  7 |        162
  8 |        212
  9 |        333
 10 |        577
(10 rows)
*/

/*
  Cool.  Seems about what we'd expect.  Now that we can create as many tables as we want, we need a way to query them and test our join performance.  To avoid writing these queries by hand, let's make another function to write them for us.  We'll tell it how many tables to make the query for, and that's it.
*/

DROP FUNCTION IF EXISTS get_query;
CREATE FUNCTION get_query(num_tables integer) RETURNS text AS $function_text$
DECLARE
    first_part text;
    second_part text;
    third_part text;
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
                t%2$s.id = t%1$s.table_%2$s_id;$query$, num_tables, num_tables-1);

RETURN first_part || second_part || third_part;
END;
$function_text$ LANGUAGE plpgsql;

/*
  And let's take a look at what this produces.
  psql -Aqt -h localhost -d join_test < join_test.sql
*/


SELECT get_query(10);

/*
        SELECT
            count(*)
        FROM
            table_1 AS t1 INNER JOIN
            table_2 AS t2 ON
                t1.id = t2.table_1_id INNER JOIN
            table_3 AS t3 ON
                t2.id = t3.table_2_id INNER JOIN
            table_4 AS t4 ON
                t3.id = t4.table_3_id INNER JOIN
            table_5 AS t5 ON
                t4.id = t5.table_4_id INNER JOIN
            table_6 AS t6 ON
                t5.id = t6.table_5_id INNER JOIN
            table_7 AS t7 ON
                t6.id = t7.table_6_id INNER JOIN
            table_8 AS t8 ON
                t7.id = t8.table_7_id INNER JOIN
            table_9 AS t9 ON
                t8.id = t9.table_8_id INNER JOIN
            table_10 AS t10 ON
                t9.id = t10.table_9_id;

 count 
-------
  10000
(1 row)
*/

/*
  Great.  Let's take a minute to consider what we're asking postgres to do when we run this query.  We're asking, how many rows in table_10 have table_9_id values that are in table_9, which have table_8_id rows that are in table_8, which have table_7_id rows that are in table_7... all the way down to table_1.  10,000 rows in each.

  And it does it all in 70ms.  Not bad!  Let's run the same query with 
*/
