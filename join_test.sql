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

  And it does it all in 70ms.  Not bad!  Let's throw an EXPLAIN ANALYZE on it and see what it's doing.

join_test=>
        EXPLAIN ANALYZE
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
                                                                                   QUERY PLAN                                                                                   
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=3758.69..3758.70 rows=1 width=8) (actual time=70.710..70.710 rows=1 loops=1)
   ->  Hash Join  (cost=2430.00..3733.69 rows=10000 width=0) (actual time=36.581..69.805 rows=10000 loops=1)
         Hash Cond: (t9.id = t10.table_9_id)
         ->  Hash Join  (cost=2160.00..3326.19 rows=10000 width=4) (actual time=31.974..61.594 rows=10000 loops=1)
               Hash Cond: (t8.id = t9.table_8_id)
               ->  Hash Join  (cost=1890.00..2918.69 rows=10000 width=4) (actual time=27.848..53.626 rows=10000 loops=1)
                     Hash Cond: (t2.table_1_id = t1.id)
                     ->  Hash Join  (cost=1620.00..2522.45 rows=10000 width=8) (actual time=24.642..46.681 rows=10000 loops=1)
                           Hash Cond: (t3.table_2_id = t2.id)
                           ->  Hash Join  (cost=1350.00..2126.21 rows=10000 width=8) (actual time=19.712..38.372 rows=10000 loops=1)
                                 Hash Cond: (t4.table_3_id = t3.id)
                                 ->  Hash Join  (cost=1080.00..1729.96 rows=10000 width=8) (actual time=15.269..30.382 rows=10000 loops=1)
                                       Hash Cond: (t5.table_4_id = t4.id)
                                       ->  Hash Join  (cost=810.00..1333.72 rows=10000 width=8) (actual time=10.864..22.552 rows=10000 loops=1)
                                             Hash Cond: (t6.table_5_id = t5.id)
                                             ->  Hash Join  (cost=540.00..937.48 rows=10000 width=8) (actual time=7.187..15.446 rows=10000 loops=1)
                                                   Hash Cond: (t7.table_6_id = t6.id)
                                                   ->  Hash Join  (cost=270.00..541.24 rows=10000 width=8) (actual time=3.628..8.398 rows=10000 loops=1)
                                                         Hash Cond: (t8.table_7_id = t7.id)
                                                         ->  Seq Scan on table_8 t8  (cost=0.00..145.00 rows=10000 width=8) (actual time=0.008..1.320 rows=10000 loops=1)
                                                         ->  Hash  (cost=145.00..145.00 rows=10000 width=8) (actual time=3.556..3.556 rows=10000 loops=1)
                                                               Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                                               ->  Seq Scan on table_7 t7  (cost=0.00..145.00 rows=10000 width=8) (actual time=0.010..1.662 rows=10000 loops=1)
                                                   ->  Hash  (cost=145.00..145.00 rows=10000 width=8) (actual time=3.491..3.491 rows=10000 loops=1)
                                                         Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                                         ->  Seq Scan on table_6 t6  (cost=0.00..145.00 rows=10000 width=8) (actual time=0.009..1.654 rows=10000 loops=1)
                                             ->  Hash  (cost=145.00..145.00 rows=10000 width=8) (actual time=3.605..3.605 rows=10000 loops=1)
                                                   Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                                   ->  Seq Scan on table_5 t5  (cost=0.00..145.00 rows=10000 width=8) (actual time=0.013..1.694 rows=10000 loops=1)
                                       ->  Hash  (cost=145.00..145.00 rows=10000 width=8) (actual time=4.244..4.244 rows=10000 loops=1)
                                             Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                             ->  Seq Scan on table_4 t4  (cost=0.00..145.00 rows=10000 width=8) (actual time=0.025..1.896 rows=10000 loops=1)
                                 ->  Hash  (cost=145.00..145.00 rows=10000 width=8) (actual time=4.361..4.361 rows=10000 loops=1)
                                       Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                       ->  Seq Scan on table_3 t3  (cost=0.00..145.00 rows=10000 width=8) (actual time=0.011..1.939 rows=10000 loops=1)
                           ->  Hash  (cost=145.00..145.00 rows=10000 width=8) (actual time=4.838..4.838 rows=10000 loops=1)
                                 Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                 ->  Seq Scan on table_2 t2  (cost=0.00..145.00 rows=10000 width=8) (actual time=0.017..2.221 rows=10000 loops=1)
                     ->  Hash  (cost=145.00..145.00 rows=10000 width=4) (actual time=3.134..3.134 rows=10000 loops=1)
                           Buckets: 16384  Batches: 1  Memory Usage: 480kB
                           ->  Seq Scan on table_1 t1  (cost=0.00..145.00 rows=10000 width=4) (actual time=0.009..1.312 rows=10000 loops=1)
               ->  Hash  (cost=145.00..145.00 rows=10000 width=8) (actual time=4.043..4.043 rows=10000 loops=1)
                     Buckets: 16384  Batches: 1  Memory Usage: 519kB
                     ->  Seq Scan on table_9 t9  (cost=0.00..145.00 rows=10000 width=8) (actual time=0.013..1.898 rows=10000 loops=1)
         ->  Hash  (cost=145.00..145.00 rows=10000 width=4) (actual time=4.501..4.501 rows=10000 loops=1)
               Buckets: 16384  Batches: 1  Memory Usage: 480kB
               ->  Seq Scan on table_10 t10  (cost=0.00..145.00 rows=10000 width=4) (actual time=0.016..1.997 rows=10000 loops=1)
 Planning time: 10.086 ms
 Execution time: 71.126 ms
(49 rows)
*/

/*
  Huh.  A bunch of sequential scans.  Weird, right?  Why isn't it using the indexes we created?  Let's force postgresql's hand by disabling sequential scans and trying again.
*/

SET enable_seqscan = OFF;

/*
join_test=>
        EXPLAIN ANALYZE
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
                                                                                          QUERY PLAN                                                                                           
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=5511.03..5511.04 rows=1 width=8) (actual time=76.564..76.564 rows=1 loops=1)
   ->  Hash Join  (cost=4146.85..5486.03 rows=10000 width=0) (actual time=50.312..75.740 rows=10000 loops=1)
         Hash Cond: (t8.id = t9.table_8_id)
         ->  Hash Join  (cost=3103.28..4304.97 rows=10000 width=4) (actual time=31.301..53.573 rows=10000 loops=1)
               Hash Cond: (t2.table_1_id = t1.id)
               ->  Hash Join  (cost=2659.99..3735.44 rows=10000 width=8) (actual time=26.329..45.599 rows=10000 loops=1)
                     Hash Cond: (t3.table_2_id = t2.id)
                     ->  Hash Join  (cost=2216.71..3165.92 rows=10000 width=8) (actual time=21.118..37.545 rows=10000 loops=1)
                           Hash Cond: (t4.table_3_id = t3.id)
                           ->  Hash Join  (cost=1773.43..2596.39 rows=10000 width=8) (actual time=16.682..30.121 rows=10000 loops=1)
                                 Hash Cond: (t5.table_4_id = t4.id)
                                 ->  Hash Join  (cost=1330.14..2026.86 rows=10000 width=8) (actual time=12.393..22.939 rows=10000 loops=1)
                                       Hash Cond: (t6.table_5_id = t5.id)
                                       ->  Hash Join  (cost=886.86..1457.34 rows=10000 width=8) (actual time=8.211..15.944 rows=10000 loops=1)
                                             Hash Cond: (t7.table_6_id = t6.id)
                                             ->  Hash Join  (cost=443.57..887.81 rows=10000 width=8) (actual time=4.030..9.029 rows=10000 loops=1)
                                                   Hash Cond: (t8.table_7_id = t7.id)
                                                   ->  Index Scan using table_8_pkey on table_8 t8  (cost=0.29..318.29 rows=10000 width=8) (actual time=0.013..1.994 rows=10000 loops=1)
                                                   ->  Hash  (cost=318.29..318.29 rows=10000 width=8) (actual time=3.961..3.961 rows=10000 loops=1)
                                                         Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                                         ->  Index Scan using table_7_pkey on table_7 t7  (cost=0.29..318.29 rows=10000 width=8) (actual time=0.011..2.253 rows=10000 loops=1)
                                             ->  Hash  (cost=318.29..318.29 rows=10000 width=8) (actual time=4.122..4.122 rows=10000 loops=1)
                                                   Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                                   ->  Index Scan using table_6_pkey on table_6 t6  (cost=0.29..318.29 rows=10000 width=8) (actual time=0.014..2.335 rows=10000 loops=1)
                                       ->  Hash  (cost=318.29..318.29 rows=10000 width=8) (actual time=4.117..4.117 rows=10000 loops=1)
                                             Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                             ->  Index Scan using table_5_pkey on table_5 t5  (cost=0.29..318.29 rows=10000 width=8) (actual time=0.011..2.335 rows=10000 loops=1)
                                 ->  Hash  (cost=318.29..318.29 rows=10000 width=8) (actual time=4.229..4.229 rows=10000 loops=1)
                                       Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                       ->  Index Scan using table_4_pkey on table_4 t4  (cost=0.29..318.29 rows=10000 width=8) (actual time=0.018..2.407 rows=10000 loops=1)
                           ->  Hash  (cost=318.29..318.29 rows=10000 width=8) (actual time=4.367..4.367 rows=10000 loops=1)
                                 Buckets: 16384  Batches: 1  Memory Usage: 519kB
                                 ->  Index Scan using table_3_pkey on table_3 t3  (cost=0.29..318.29 rows=10000 width=8) (actual time=0.014..2.469 rows=10000 loops=1)
                     ->  Hash  (cost=318.29..318.29 rows=10000 width=8) (actual time=5.118..5.118 rows=10000 loops=1)
                           Buckets: 16384  Batches: 1  Memory Usage: 519kB
                           ->  Index Scan using table_2_pkey on table_2 t2  (cost=0.29..318.29 rows=10000 width=8) (actual time=0.017..2.766 rows=10000 loops=1)
               ->  Hash  (cost=318.29..318.29 rows=10000 width=4) (actual time=4.891..4.891 rows=10000 loops=1)
                     Buckets: 16384  Batches: 1  Memory Usage: 480kB
                     ->  Index Only Scan using table_1_pkey on table_1 t1  (cost=0.29..318.29 rows=10000 width=4) (actual time=0.022..2.729 rows=10000 loops=1)
                           Heap Fetches: 10000
         ->  Hash  (cost=918.57..918.57 rows=10000 width=4) (actual time=18.863..18.863 rows=10000 loops=1)
               Buckets: 16384  Batches: 1  Memory Usage: 480kB
               ->  Merge Join  (cost=0.57..918.57 rows=10000 width=4) (actual time=0.080..15.814 rows=10000 loops=1)
                     Merge Cond: (t9.id = t10.table_9_id)
                     ->  Index Scan using table_9_pkey on table_9 t9  (cost=0.29..318.29 rows=10000 width=8) (actual time=0.032..2.723 rows=10000 loops=1)
                     ->  Index Only Scan using table_10_table_9_id_idx on table_10 t10  (cost=0.29..450.28 rows=10000 width=4) (actual time=0.042..7.138 rows=10000 loops=1)
                           Heap Fetches: 10000
 Planning time: 4.555 ms
 Execution time: 76.790 ms
(49 rows)
*/

/*
  There we go, index scans just like we wanted.  Except... it didn't actually get any faster.  What the heck is going on here!?

  To understand why, we need to remember that an index works by pointing us to where rows with a certain value lie on disk.  Just like an index in a book can save us from reading every page just to find where one topic is mentioned, the database can avoid reading every row (a.k.a. a sequential scan) to find where just a few rows are located.  When the table is large, and we're only interested in a small subset of it, an index can be a huge speedup.

  But what if we wanted to cover every topic in the book?  Obviously, we would start on page one and continue reading till we're done.  End of story.  We certainly wouldn't use the index to find where the first topic is, skip to that page, read the topic, use the index to find where the second topic is, skip to that page, read the topic, etc.  Not only would this be a bizzare way to read a book, it would be much slower than just reading it start to finish.

  Even if we were only interested in 80% of the topics in a book, we'd probably still just tough it out and read it start to finish, versus using the index to jump around.

  In order for an index in a book or a database to be worthwhile, we really have to be interested in a small subset of the whole thing.
*/
