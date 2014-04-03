--
-- Test querying cstore_fdw tables.
--

-- Settings to make the result deterministic
SET datestyle = "ISO, YMD";

-- Query uncompressed data
SELECT count(*) FROM contestant;
SELECT avg(rating), stddev_samp(rating) FROM contestant;
SELECT country, avg(rating) FROM contestant WHERE rating > 2200
	GROUP BY country ORDER BY country;
SELECT * FROM contestant ORDER BY handle;

-- Query compressed data
SELECT count(*) FROM contestant_compressed;
SELECT avg(rating), stddev_samp(rating) FROM contestant_compressed;
SELECT country, avg(rating) FROM contestant_compressed WHERE rating > 2200
	GROUP BY country ORDER BY country;
SELECT * FROM contestant_compressed ORDER BY handle;
