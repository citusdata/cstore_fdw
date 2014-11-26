--
-- Testing insert on cstore_fdw tables.
--

-- Settings to make the result deterministic
SET datestyle = "ISO, YMD";

CREATE FOREIGN TABLE cstore_table (a int) SERVER cstore_server;


-- test single row inserts fail
select count(*) from cstore_table;
insert into cstore_table values(1);
select count(*) from cstore_table;


-- test inserting from another table succeed
CREATE TABLE regular_table (a int);
select count(*) from regular_table;
insert into regular_table values(1);
select count(*) from regular_table;

insert into cstore_table select * from regular_table;
select count(*) from cstore_table;


drop table regular_table;
drop foreign table cstore_table;
