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
    start = time.time()

    for site in sites:
        #import pdb; pdb.set_trace()
        logging.debug('Scrapping {}'.format(site.upper()))
        # not UTC but EET
        now = datetime.now()
        now_date = str(now.date())
        file_name = site + '_' + now_date + '.tsv'

        offers = get_new_offers(site)
        if offers is None:
            continue

        offers['measurement_day'] = datetime.now().strftime("%Y-%m-%d")

        # accomodate all columns across all datasets
        for col in COLUMNS:
            if col not in offers.columns:
                offers[col] = None

        offers = offers[COLUMNS]

        #import pdb; pdb.set_trace()
        if not is_run_locally:
    
            csv_buffer = StringIO()
            offers.to_csv(csv_buffer, sep='\t', encoding='utf-16', index=False)
            logging.debug(site + ' has ' + str(offers.shape[0]) + ' offers.\n')
            session = boto3.session.Session(profile_name='aero')

            s3 = session.resource('s3')
            s3.Object(DESTINATION_BUCKET, 'raw/' + site + '/' + now_date + '/' + file_name).put(Body=csv_buffer.getvalue())
        else:
            if not os.path.exists('output'):
                os.mkdir('output')
            offers.to_csv('output/' + file_name, sep='\t', encoding='utf-16', index=False)

    logging.debug('Processing took {} hours.'.format(str(timedelta(seconds=time.time() - start))))
    print('Processing took {} hours'.format(str(timedelta(seconds=time.time() - start))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-is_run_locally', required=False, help="MMDD", default=False)
    parser.add_argument('-sites', required=False, help="site1, site2", default="address, arco, etuovi, holmes, imoteka, superimoti, vuokraovi, yavlena")
    parsed = parser.parse_args()
    is_run_locally = bool(parsed.is_run_locally)
    sites = [s.strip() for s in parsed.sites.split(',')]
    save_file(is_run_locally, sites)


