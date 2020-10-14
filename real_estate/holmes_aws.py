#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import bs4
import boto3
import requests
import pandas as pd
import re
import os
from tqdm import tqdm
from io import StringIO
import json
import urllib
from helpers import clean_text, replace_month_with_digit, months
from datetime import datetime


base_url = 'http://sofia.holmes.bg'
links_file = "holmes_links_"
offers_file = "holmes_"
DESTINATION_BUCKET = "real-estate-scrapping"


def get_neighbourhood_links():
    search_page_template = '/pcgi/home.cgi?act=5&f1=0&f2=1&f3=0&f4=%E3%F0%E0%E4%20%D1%EE%F4%E8%FF'
    # page_link = base_url + search_page_template
    resp = requests.get(base_url)
    page = bs4.BeautifulSoup(resp.content.decode('cp1251'), 'html')
    neighbourhoods = page.findAll('a',
                                  href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f4=град(?:\s)?София.*f5=[А-Яа-я0-9\s]+'),
                                  # text=re.compile('[\d]+'),
                                  # attrs={'class': 'linkSearch'}
                                  )
    neighbourhoods = [re.search('&f5=(.*)$', i['href']).group(1) for i in neighbourhoods]
    neighbourhoods = list(set([(n, base_url + search_page_template + '&f5=' + urllib.parse.quote(n.encode('cp1251'))) for n in neighbourhoods]))
    return neighbourhoods


def get_all_links_for_each_neighbourhood(neighbourhoods, current_date):
    links = []

    for n in neighbourhoods:
        pages = set([1])
        last_page = False
        page_link = n[1]

        while not last_page:
            resp = requests.get(page_link)
            page = bs4.BeautifulSoup(resp.content.decode('cp1251'), 'html')

            visible_pages = page.findAll('a', href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f6='), text=re.compile('[\d]+')) #, attrs={'class': 'pageNumbers'})
            visible_pages = [int(i.text) for i in visible_pages]

            next_page = page.findAll('a', href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f6='), text=re.compile('^(?![\d]+)')) #, attrs={'class': 'pageNumbers'})
            href = next_page[0]['href'] if len(next_page) > 0 else None
            page_number = int(re.search('([\d]+)$', href).group(1)) if href is not None else None

            if len(visible_pages) > 0 and len(next_page) > 0 and page_number > max(pages):
                pages.update(set([page_number]))
                page_link = n[1] + '&f6={}'.format(page_number)
            else:
                last_page = True

            pages.update(set(visible_pages))

        search_pages = [n[1] + '&f6={}'.format(p) for p in pages]

        for p in search_pages:
            resp = requests.get(p)
            page = bs4.BeautifulSoup(resp.content.decode('cp1251'), 'html')

            visible_links = page.findAll('a', href=re.compile('/pcgi/home\.cgi.*act=3.*adv=[\w\d]+$')) #, attrs={'class': 'linkLocatList'})
            visible_links = [(base_url + i['href'], n[0]) for i in visible_links]
            links = links + visible_links

        print('{}: {}'.format(n[0], len(search_pages)))

    links = list(set(links))
    links_df = pd.DataFrame(links, columns=['link', 'neighbourhood'])
    """if not os.path.exists('output'):
        os.mkdir('output')
    links_df.to_csv('output/' + links_file + current_date + '.tsv', sep='\t', index=False)"""

    return links_df['link'].values,  links_df['neighbourhood'].values


def gather_new_articles(current_date):
    """if os.path.exists('output/' + links_file + current_date + '.tsv'):
        links = list(pd.read_csv('output/' + links_file + current_date + '.tsv', sep='\t')['link'].values)
        nbbhds = list(pd.read_csv('output/' + links_file + current_date + '.tsv', sep='\t')['neighbourhood'].values)
    else:"""
    
    neighbourhoods = get_neighbourhood_links()
    links, nbbhds = get_all_links_for_each_neighbourhood(neighbourhoods, current_date)

    #import pdb; pdb.set_trace()
    offers = crawlLinks(links, nbbhds, current_date)
    offers = offers[['link', 'title', 'address', 'details', 'neighbourhood', 'lon', 'lat', 'id', 'price', 'price_sqm', 'area', 'floor', 'description', 'views', 'date', 'agency', 'poly']]	

    return offers


def send_to_rds(dd):
    with open('connection_rds.txt', 'r') as f:
        DATABASE_URI = f.read()
        engine = sal.create_engine(DATABASE_URI)
        conn = engine.connect()

    d['title'] = d['title'].apply(lambda x: x.split('   ')[0].lower())
    d['views'] = d['views'].apply(lambda x: str(x).replace('пъти', ''))
    d.rename(columns={"neighbourhood":'place'}, inplace=True)
    d.drop(columns=['poly', 'agency'], inplace=True)
    d['measurement_day'] = date

    types = {}
    for col in d.columns:
        types[col] = sal.types.String()

    engine.execute('DELETE FROM holmes_import')
    
    conn_raw = engine.raw_connection()
    cur = conn_raw.cursor()
    output = io.StringIO()
    d.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'holmes_import', null="")
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
    INSERT INTO holmes (link, title, address, details, place, lon, lat, id, price, price_sqm, area, floor, description, views, date, measurement_day)
    SELECT * FROM holmes_import_casted
    """
    engine.execute(ingest_query)


def get_agency(page):
    ag = page.find_all('div', {'class': 'AG'})
    if len(ag) < 1:
        return ''
     
    return ag[0].find_all('a')[0].text


def get_desc(page):
    desc_start_phrase = '<div>Допълнителна информация:</div>'
    desc_end_phrase = '<'
    desc_start_ind = str(page).find(desc_start_phrase) + len(desc_start_phrase)
    desc_end_ind = str(page).find(desc_end_phrase, desc_start_ind)
    desc = str(page)[desc_start_ind:desc_end_ind] if desc_start_ind > 50 else ''

    return clean_text(desc)


def get_date(date):
    date = date.replace('Публикувана в ', '')
    month_name = [m for m in months.keys() if m in date][0]
    # "15:54 на 21 февруари, 2020 год."
    # "12:03 на 3 септември, 2020 год."
    articleDate = date.replace(month_name,
                                      replace_month_with_digit(month_name)) if month_name is not None else date
    articleDate = pd.to_datetime(articleDate, format='%H:%M на %d %m, %Y год.')

    return articleDate


def get_details(elements):
    dict = {}
    if len(elements) % 2 != 0:
        return None
    else:
        for i in range(0, len(elements), 2):
            dict[elements[i].text] = elements[i+1].text

        return dict


def crawlLinks(links, nbbhds, current_date):
    offers = pd.DataFrame(data={'link': []})
    visited_offers = []
	
    for ind in tqdm(range(len(links))):
        link = links[ind]
        # temp
        if link in visited_offers:
            continue
        try:
            resp = requests.get(link)
            page = bs4.BeautifulSoup(resp.content.decode('cp1251'), 'html.parser')
            page = page.find_all('div', attrs={'class': 'content'})[0]

            id = re.search('=([\d\w]+)$', link).group(1)
            lon = page.find_all('input', attrs={'name': 'mapn', 'type': 'hidden'})[0]['value'].split(',')[0] \
                if len(page.find_all('input', attrs={'name': 'mapn', 'type': 'hidden'})) > 0 \
                else ''
            lat = page.find_all('input', attrs={'name': 'mapn', 'type': 'hidden'})[0]['value'].split(',')[1] \
                if len(page.find_all('input', attrs={'name': 'mapn', 'type': 'hidden'})) > 0 \
                else ''

            address = clean_text(page.find_all('div', attrs={'class': 'title'})[0].find_all('span')[0].text.replace('Виж на картата', '')) \
                if len(page.find_all('div', attrs={'class': 'title'})[0].find_all('span')) > 0 \
                else ''
            poly = clean_text(page.find_all('input', attrs={'name': 'p', 'type': 'hidden'})[0]['value']) \
                if len(page.find_all('input', attrs={'name': 'p', 'type': 'hidden'})) > 0 \
                else ''
            details_li = page.find_all('ul', attrs={'class': 'param'})[0].find_all('li')
            details = get_details(details_li)

            price = clean_text(page.find_all('div', {'id': re.compile('^price$')})[0].text)
            price_sq = clean_text(page.find_all('em', {'id': re.compile('^price_kv$')})[0].text)
            agency = get_agency(page)

            views = page.find_all('span', {'class': 'num'})[0].text.replace(' ', '')
            date = page.find_all('span', {'class': 'date'})[0].text
            date = get_date(date)
            desc = get_desc(page)
            area = details['Квадратура'] if 'Квадратура' in details.keys() else ''
            floor = details['Етаж'] if 'Етаж' in details.keys() else ''

            title = clean_text(page.find_all('div', attrs={'class': 'title'})[0].text) \
                if len(page.find_all('div', attrs={'class': 'title'})) > 0 \
                else ''

            current_offer = pd.DataFrame(data={'link': link,
                                               'title': title,
                                               'address': address,
                                               'details': json.dumps(details, ensure_ascii=False),
                                               'neighbourhood': nbbhds[ind].split(',')[0],
                                               'lon': lon,
                                               'lat': lat,
                                               'id': id,
                                               'price': price,
                                               'price_sqm': price_sq,
                                               'area': area,
                                               'floor': floor,
                                               'description': desc,
                                               'views': views,
                                               'date': date,
                                               'agency': agency,
                                               'poly': poly}, index=[0])
            #import pdb; pdb.set_trace()

            offers = pd.concat([offers, current_offer], ignore_index=True)

        except Exception as e:
            print(e)
            continue

    return offers


if __name__ == '__main__':
	current_date = str(datetime.now().date())
	df = gather_new_articles(current_date)
    send_to_rds(df)