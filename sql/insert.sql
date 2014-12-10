--
-- Testing insert on cstore_fdw tables.
--

CREATE FOREIGN TABLE test_insert_command (a int) SERVER cstore_server;

-- test single row inserts fail
select count(*) from test_insert_command;
insert into test_insert_command values(1);
select count(*) from test_insert_command;

insert into test_insert_command default values;
select count(*) from test_insert_command;

-- test inserting from another table succeed
CREATE TABLE test_insert_command_data (a int);

select count(*) from test_insert_command_data;
insert into test_insert_command_data values(1);
select count(*) from test_insert_command_data;

insert into test_insert_command select * from test_insert_command_data;
select count(*) from test_insert_command;

drop table test_insert_command_data;
drop foreign table test_insert_command;
