--
-- Test utility functions for cstore_fdw tables.
--

CREATE FOREIGN TABLE empty_table (a int) SERVER cstore_server;
CREATE FOREIGN TABLE table_with_data (a int) SERVER cstore_server;

COPY table_with_data FROM STDIN;
1
2
3
\.

SELECT cstore_table_size('empty_table') < cstore_table_size('table_with_data');

DROP FOREIGN TABLE empty_table;
DROP FOREIGN TABLE table_with_data;
