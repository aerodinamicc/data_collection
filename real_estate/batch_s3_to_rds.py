#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#import psycopg2
import json
import boto3
import sqlalchemy as sal
import pandas as pd
import os
import re
import io
import aws_interface as aws

_BUCKET = "real-estate-scrapping"
_PATH = "queries/"


with open('connection_rds.txt', 'r') as f:
    DATABASE_URI = f.read()
engine = sal.create_engine(DATABASE_URI)
conn = engine.connect()

#query RDS
distinct_days = pd.read_sql("select distinct measurement_day from daily_measurements", engine)['measurement_day'].values
incl_inline_query = "('" + "', '".join(distinct_days) + "')"

#QUERY ATHENA
athena_query = "SELECT * FROM real_estate_db.raw_measurements"

if len(distinct_days) > 0:
	athena_query +=  " WHERE measurement_day NOT IN {}".format(incl_inline_query)

session = boto3.session.Session(profile_name='aero')
#athena_client = session.client('athena', region_name='eu-central-1')
#s3_object = aws.query_athena(athena_client, athena_query, _BUCKET, _PATH)

s3_object = '913daa2b-e31d-4125-a90b-a266621c639d.csv'

s3_client = session.client('s3')
df = aws.fetch_from_s3(s3_client, s3_object, _BUCKET, _PATH)

#CLEANING
df['type'] = df['type'].map(str).apply(lambda x: x.lower().strip().replace('1-', 'едно').replace('2-', 'дву').replace('3-', 'три').replace('4-', 'четири').replace(' апартамент', ''))
df['is_apartment'] = df['type'].map(str).apply(lambda x: re.search('(?:стаен|мезонет|ателие)', x) is not None)
df['country'] = df['link'].map(str).apply(lambda x: 'fi' if 'etuovi.com' in x or 'www.vuokraovi.com' in x else 'bg')
df['place'] = df['place'].map(str).apply(lambda x: x.lower().strip().replace('гр. софия', '').replace('софийска област', '').replace('българия', '').replace('/', '').replace(',', '').replace('близо до', ''))
df['price'] = df['price'].map(str).apply(lambda x: re.search('([\d\.]{3,100})', x.replace(' ', '')).group(1) if re.search('([\d\.]{3,100})', x.replace(' ', '')) is not None else None)
df['area'] = df['area'].map(str).apply(lambda x: re.search('([\d\.]{3,100})', x.replace(' ', '')).group(1) if re.search('([\d\.]{3,100})', x.replace(' ', '')) is not None else None)
df['year'] = df['year'].map(str).apply(lambda x: re.search('([\d]{4})', x.replace(' ', '')).group(1) if re.search('([\d]{4})', x.replace(' ', '')) is not None else None)
df['lon'] = df['lon'].map(str).apply(lambda x: x.replace(',', '.'))
df['lat'] = df['lat'].map(str).apply(lambda x: x.replace(',', '.'))

engine.execute('DELETE FROM daily_import')

#CREATE IMPORT TABLES

import_creation = """
CREATE TABLE if not exists daily_import (
	link VARCHAR,
	is_for_sale VARCHAR,
	price VARCHAR,
	labels VARCHAR, 
	views VARCHAR,
	measurement_day VARCHAR,
	country VARCHAR,
	id VARCHAR PRIMARY KEY,
	type VARCHAR,
	city VARCHAR,
	place VARCHAR,
	is_apartment VARCHAR,
	area VARCHAR,
	details VARCHAR,
	year VARCHAR,
	available_from VARCHAR,
	lon VARCHAR,
	lat VARCHAR
);
"""

engine.execute(import_creation)

#CHOPING AND SENDING FILES OVER
conn_raw = engine.raw_connection()
cur = conn_raw.cursor()
table_columns = pd.read_sql("select * from daily_import limit 5", engine).columns

def send_to_rds(df, cur, conn_raw, table, split_start, split_end):
	output = io.StringIO()
	temp = df[split_start:split_end][table_columns]
	print('Remaining: ' + str(df.shape[0] - split_end))
	#import pdb;pdb.set_trace()
	temp.to_csv(output, sep='\t', header=False, index=False)
	output.seek(0)
	contents = output.getvalue()
	cur.copy_from(output, table, null= "")
	conn_raw.commit()

def chop_big_files(df, cur, conn_raw, table):
	up_till = 100000
	step = 100000

	while up_till < df.shape[0]:
		send_to_rds(df, cur, conn_raw, table, up_till - step, up_till)
		up_till += step

	send_to_rds(df, cur, conn_raw, table, up_till - step, df.shape[0])

for d, table in [(df, 'daily_import')]:
	chop_big_files(d, cur, conn_raw, table)

#CASTING

engine.execute('DROP TABLE IF EXISTS daily_import_casted')

casted_query = """
CREATE TABLE daily_import_casted AS (
SELECT
	id,
	is_for_sale::boolean,
	price::float,
	labels,
	views::float,
	measurement_day,
	link,country,type,city,place,
	is_apartment::boolean,
	area::float,
	details,
	year::float,
	available_from,
	lon::float,
	lat::float
FROM daily_import
)
"""

engine.execute('DROP TABLE IF EXISTS daily_import_casted')
engine.execute(sal.text(casted_query))

#INGESTING

ingest_metadata = """
INSERT INTO daily_metadata (link, country, id, type, is_apartment, city, place, area, details, year, available_from, lon, lat)
SELECT DISTINCT link, country, id, type, is_apartment, city, place, area, details, year, available_from, lon, lat
FROM daily_import_casted
ON CONFLICT (link) DO NOTHING
"""

ingest_measurements = """
INSERT INTO daily_measurements (id, is_for_sale, price, labels, views, measurement_day)
SELECT id, is_for_sale, price, labels, views, measurement_day FROM daily_import_casted
ON CONFLICT DO NOTHING
"""

engine.execute(ingest_metadata)
engine.execute(ingest_measurements)