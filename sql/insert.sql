--
-- Testing insert on cstore_fdw tables.
--

CREATE FOREIGN TABLE test_cstore_table (a int) SERVER cstore_server;


-- test single row inserts fail
select count(*) from test_cstore_table;
insert into test_cstore_table values(1);
select count(*) from test_cstore_table;

insert into test_cstore_table default values;
select count(*) from test_cstore_table;

-- test inserting from another table succeed
CREATE TABLE test_regular_table (a int);
select count(*) from test_regular_table;
insert into test_regular_table values(1);
select count(*) from test_regular_table;

insert into test_cstore_table select * from test_regular_table;
select count(*) from test_cstore_table;

drop table test_regular_table;
drop foreign table test_cstore_table;
