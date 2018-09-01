DROP TABLE IF EXISTS foo;
CREATE TABLE foo (
    id int not null,
    label text
);


DROP TABLE IF EXISTS bar;
CREATE TABLE bar (
    foo_id integer references foo (id)
);
