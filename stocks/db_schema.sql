----
-- sp500 companies
----
DROP TABLE IF EXISTS sp500_companies;

CREATE TABLE sp500_companies (
	symbol VARCHAR(5) PRIMARY KEY,
	name VARCHAR(100),
	weight FLOAT,
	details jsonb
);

----
-- price actions
----

DROP TABLE IF EXISTS price_actions;

CREATE TABLE price_actions (
	symbol VARCHAR(5),
	at_date date,
	volume NUMERIC,
	at_open FLOAT,
	at_close FLOAT,
	at_close_adj FLOAT,
	high FLOAT,
	low FLOAT
);

