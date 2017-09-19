--
-- Test the CREATE statements related to cstore_fdw.
--


-- Install cstore_fdw
CREATE EXTENSION cstore_fdw;

CREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;


-- Validator tests
CREATE FOREIGN TABLE test_validator_invalid_option () 
	SERVER cstore_server 
	OPTIONS(bad_option_name '1'); -- ERROR

CREATE FOREIGN TABLE test_validator_invalid_stripe_row_count () 
	SERVER cstore_server
	OPTIONS(stripe_row_count '0'); -- ERROR

CREATE FOREIGN TABLE test_validator_invalid_block_row_count () 
	SERVER cstore_server
	OPTIONS(block_row_count '0'); -- ERROR

CREATE FOREIGN TABLE test_validator_invalid_compression_type () 
	SERVER cstore_server
	OPTIONS(filename 'data.cstore', compression 'invalid_compression'); -- ERROR

-- filename option is not supported anymore
CREATE FOREIGN TABLE test_invalid_file_path ()
	SERVER cstore_server
	OPTIONS(filename 'data.cstore'); --ERROR

-- valid logging options are true, and false
CREATE FOREIGN TABLE test_invalid_log_option ()
	SERVER cstore_server
	OPTIONS(logging 'enabled'); --ERROR

-- Create uncompressed table
CREATE FOREIGN TABLE contestant (handle TEXT, birthdate DATE, rating INT,
	percentile FLOAT, country CHAR(3), achievements TEXT[])
	SERVER cstore_server;

-- Create compressed table with automatically determined file path
CREATE FOREIGN TABLE contestant_compressed (handle TEXT, birthdate DATE, rating INT,
	percentile FLOAT, country CHAR(3), achievements TEXT[])
	SERVER cstore_server
	OPTIONS(compression 'pglz');

-- Test that querying an empty table works
ANALYZE contestant;
SELECT count(*) FROM contestant;
