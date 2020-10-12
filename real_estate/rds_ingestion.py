#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#import psycopg2
import json
import boto3
import sqlalchemy as sal
import pandas as pd
import os
import io

_BUCKET = 'real-estate-scrapping'

# Local implementation

with open('connection_rds.txt', 'r') as f:
    DATABASE_URI = f.read()
engine = sal.create_engine(DATABASE_URI)
conn = engine.connect()

distinct_days = pd.read_sql("select distinct measurement_day from holmes", engine)['measurement_day'].values

#session = boto3.session.Session(profile_name='aero')
s3 = boto3.client('s3')
my_bucket = s3.list_objects(Bucket=_BUCKET, Prefix='holmes_weekly_detailed')
files = [obj['Key'] for obj in my_bucket['Contents']]

new_dates = [(f, f[f.find('holmes_20') + len('holmes_'):f.find('.tsv')]) \
                for f in files \
                if f[f.find('holmes_20') + len('holmes_'):f.find('.tsv')] not in distinct_days]

# POPULATING THE DB

engine.execute('DELETE FROM holmes_import')
for f, date in new_dates:
    print(f)
    obj = s3.get_object(Bucket= _BUCKET, Key= f) 

    d = pd.read_csv(obj['Body'],  sep='\t')
    d['title'] = d['title'].apply(lambda x: x.split('   ')[0].lower())
    d['views'] = d['views'].apply(lambda x: str(x).replace('пъти', ''))
    d.rename(columns={"neighbourhood":'place'}, inplace=True)
    d.drop(columns=['poly', 'agency'], inplace=True)
    d['measurement_day'] = date

    types = {}
    for col in d.columns:
        types[col] = sal.types.String()
    
    conn_raw = engine.raw_connection()
    cur = conn_raw.cursor()
    output = io.StringIO()
    d.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'holmes_import', null="") # null values become ''
    conn_raw.commit()

#CLEANING

cast_query = """
CREATE TABLE holmes_import_casted AS
SELECT 
	link, 
    title, 
	substring(address from trim(place)||'(.*)') as address, 
	replace(substring(details, 2, length(details)-2), '""', '"')::json,
	trim(place), 
    lon::float, 
    lat::float,
	id, 
	case when lower(price) like '%лв%' THEN round(replace(substring(price from '[\d\s]+'), ' ', '')::float / 1.9588, 2)
		 WHEN trim(price) = 'при запитване' THEN 0.
		 ELSE replace(substring(trim(price) from '[\d\s]+'), ' ', '')::FLOAT
		END as price,
	case when price_sqm like '%лв%' then round(substring(price_sqm from '[\d\.]+')::float / 1.9588, 2)
		 WHEN trim(price) = 'при запитване' THEN 0.
		 ELSE substring(price_sqm from '[\d\.]+')::FLOAT 
		END as price_sqm,
	substring(area from '[\d]+')::bigint as area,
	CASE WHEN LOWER(TRIM(floor)) IN ('партер', 'сутерен') then 1 ELSE substring(floor from '[\d]+')::bigint END as floor,
	description, 
    views::bigint, 
    date,
	measurement_day
FROM holmes_import
"""

engine.execute('DROP TABLE IF EXISTS holmes_import_casted')
engine.execute(sal.text(cast_query))

#INGESTING

ingest_query = """
INSERT INTO holmes (link, title, address, details, region, place, lon, lat, id, price, price_sqm, area, floor, description, views, date, measurement_day)
SELECT * FROM holmes_import_casted
"""
engine.execute(ingest_query)
