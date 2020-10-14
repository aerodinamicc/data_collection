import argparse
import os
import boto3
from datetime import datetime, timedelta
from io import StringIO
import time
import logging
import address_daily
import arco_daily
import etuovi_daily
import holmes_daily
import imoteka_daily
import superimoti_daily
import vuokraovi_daily
import yavlena_daily
    

COLUMNS = ['link', 'id', 'type', 'city', 'place', 'is_for_sale', 'price', 'area', 'details', 'labels', 'year', 'available_from', 'views', 'lon', 'lat', 'measurement_day']
MAIN_COLUMNS = ['link', 'id', 'type', 'city', 'place', 'is_for_sale', 'area']
DETAIL_COLUMNS = ['id', 'price', 'details', 'labels', 'year', 'available_from', 'views', 'lon', 'lat', 'measurement_day']
DESTINATION_BUCKET = 'real-estate-scrapping'


def get_new_offers(site):
    articles = None
    try:
        if site == 'address': 
            articles = address_daily.gather_new_articles()
        elif site == 'arco':
            articles = arco_daily.gather_new_articles()
        elif site == 'etuovi':
            articles = etuovi_daily.gather_new_articles()
        elif site == 'holmes':
            articles = holmes_daily.gather_new_articles()
        elif site == 'imoteka':
            articles = imoteka_daily.gather_new_articles()
        elif site == 'superimoti':
            articles = superimoti_daily.gather_new_articles()
        elif site == 'vuokraovi': 
            articles = vuokraovi_daily.gather_new_articles()
        elif site == 'yavlena': 
            articles = yavlena_daily.gather_new_articles()
    except:
        return None
    
    return articles


def save_file(is_run_locally, sites):
    df = pd.DataFrame()

    for site in sites:
        print('\n' + site)
        #import pdb; pdb.set_trace()
        logging.debug('Scrapping {}'.format(site.upper()))
        # not UTC but EET
        now = datetime.now()
        now_date = str(now.date())
        file_name = site + '_' + now_date + '.tsv'

        offers = get_new_offers(site)
        if offers is None:
            continue

        offers['measurement_day'] = now_date

        # accomodate all columns across all datasets
        for col in COLUMNS:
            if col not in offers.columns:
                offers[col] = None

        offers = offers[COLUMNS]
        df = pd.concat([df, offers])

    return df


def send_to_rds(df):
    with open('connection_rds.txt', 'r') as f:
        DATABASE_URI = f.read()
        engine = sal.create_engine(DATABASE_URI)

    df['type'] = df['type'].map(str).apply(lambda x: x.lower().strip().replace('1-', 'едно').replace('2-', 'дву').replace('3-', 'три').replace('4-', 'четири').replace(' апартамент', ''))
    df['is_apartment'] = df['type'].map(str).apply(lambda x: re.search('(?:стаен|мезонет|ателие)', x) is not None)
    df['country'] = df['link'].map(str).apply(lambda x: 'fi' if 'etuovi.com' in x or 'www.vuokraovi.com' in x else 'bg')
    df['place'] = df['place'].map(str).apply(lambda x: x.lower().strip().replace('гр. софия', '').replace('софийска област', '').replace('българия', '').replace('/', '').replace(',', '').replace('близо до', ''))
    df['price'] = df['price'].map(str).apply(lambda x: re.search('([\d\.]{3,100})', x.replace(' ', '')).group(1) if re.search('([\d\.]{3,100})', x.replace(' ', '')) is not None else None)
    df['area'] = df['area'].map(str).apply(lambda x: re.search('([\d\.]{3,100})', x.replace(' ', '')).group(1) if re.search('([\d\.]{3,100})', x.replace(' ', '')) is not None else None)
    df = df[df['country'] = 'bg']
    df['site'] = df['link'].apply(lambda x: re.search('.*://([^/]*)', x).group(1) if re.search('.*://([^/]*)', x) is not None else None)
    df['year'] = df['year'].map(str).apply(lambda x: re.search('([\d]{4})', x.replace(' ', '')).group(1) if re.search('([\d]{4})', x.replace(' ', '')) is not None else None)
    df['lon'] = df['lon'].map(str).apply(lambda x: x.replace(',', '.'))
    df['lat'] = df['lat'].map(str).apply(lambda x: x.replace(',', '.'))

    engine.execute('DELETE FROM daily_import')

    #IMPORTING
    conn_raw = engine.raw_connection()
    cur = conn_raw.cursor()
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(df, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'daily_import', null="")
    conn.commit()

    #CASTING

    engine.execute('DROP TABLE IF EXISTS daily_import_casted')
    casted_query = """
    CREATE TABLE daily_import_casted AS (
    SELECT
        id,
        site,
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
    engine.execute(sal.text(casted_query))

    #INGESTING
    ingest_metadata = """
    INSERT INTO daily_metadata (link, site, country, id, type, is_apartment, city, place, area, details, year, available_from, lon, lat)
    SELECT DISTINCT link, site, country, id, type, is_apartment, city, place, area, details, year, available_from, lon, lat
    FROM daily_import_casted
    ON CONFLICT (link) DO NOTHING
    """

    ingest_measurements = """
    INSERT INTO daily_measurements (site, id, is_for_sale, price, labels, views, measurement_day)
    SELECT site, id, is_for_sale, price, labels, views, measurement_day FROM daily_import_casted
    """


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-is_run_locally', required=False, help="MMDD", default=False)
    parser.add_argument('-sites', required=False, help="site1, site2", default="address, arco, holmes, imoteka, superimoti, yavlena")
    parsed = parser.parse_args()
    is_run_locally = bool(parsed.is_run_locally)
    sites = [s.strip() for s in parsed.sites.split(',')]
    df = save_file(is_run_locally, sites)
    send_to_rds(df)


