#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import bs4
import requests
import pandas as pd
import re
import json
import os
from datetime import datetime
from selenium import webdriver
from tqdm import tqdm


links_file = "imotibg_links_"
offers_file = "imotibg_"


def get_links_from_results_page(page):
    lnks = page.find_all('a', {'href': re.compile('http://www\.imotibg\.com/.*property[\d]+\.html')})
    lnks = list(set([a['href'] for a in lnks]))
    return lnks


def enrich_with_location(df, coors):
    coors.rename(columns={'lang': 'lon', 'area': 'area_code'}, inplace=True)
    coors = coors[['id', 'area_code', 'lon', 'lat']]
    df = pd.merge(df, coors, on='id', how='left')
    return df


def gather_new_articles(current_date):
    links = []
    base = 'http://www.imotibg.com/property/index/1'
    first_page = base + '?page=1'

    last_page = False

    while not last_page:
        request = requests.get(first_page)
        page = bs4.BeautifulSoup(request.text, 'html.parser')
        next_page = page.select('.next')[0]
        links = links + get_links_from_results_page(page)
        if next_page.has_attr('href'):
            first_page = base + next_page['href']
            print(first_page)
        else:
            last_page = True

    links = list(set(links))

    offers, coors = crawlLinks(links)
    offers = enrich_with_location(offers, coors)
    offers = offers[['address' , 'place', 'details', 'id', 'link', 'title', 'lon', 'lat', 'description']]
    if not os.path.isdir('output'):
        os.mkdir('output')
    offers.to_csv('output/' + offers_file + current_date + '.tsv', sep='\t', index=False)

    return offers


def crawlLinks(links):
    offers = pd.DataFrame()
    properties = []
    ids_set = set()

    # links = ['http://www.imotibg.com/prodava-tristaen-apartament-sofiya-centar-170000-eur-property599726.html']
    for link in tqdm(list(links)):
        try:
            rq = requests.get(link)
            page = bs4.BeautifulSoup(rq.content, 'html.parser', from_encoding="utf-8")

            title = page.select('.property-title')[0].select('h1')[0].text.replace('\n', '')
            id = re.search('property([\d]+)\.html', link).group(1)
            address = page.select('.property-title')[0].select('figure')[0].text.replace('\n', '').replace('Ориентир:', '') \
                                                if len(page.select('.property-title')[0].select('figure')) > 0 else None
            # София Враждебна  По договаряне 	5657
            # София - Герман  338000
            place = re.search('София(.+?)(?=По договаряне|[\d]+)', title).group(1).replace('-', '').strip() if re.search('София(.+?)(?=По договаряне|[\d]+)', title) is not None else ''
            details = {}
            if len(page.select('.section-sub')) > 0:
                imotData = page.select('.section-sub')[0].select('dt')
                imotDataValue = page.select('.section-sub')[0].select('dd')
                for i in range(0, len(imotData)):
                    details[imotData[i].text.replace('\n', '')] = imotDataValue[i].text.replace('\n', '').replace('\t', '').strip()

            desc = page.select('#description')[0].text.replace('\n:', '').replace('\t', '') if len(page.select('#description')) > 0 else None
            additional_phrase = "var aditional = '"
            additional_st = rq.text.find(additional_phrase)
            additional_end = rq.text.find("]'", additional_st)
            json_str = rq.text[additional_st + len(additional_phrase):additional_end + 1]
            additional_properties = json.loads(json_str)
            properties = properties + [p for p in additional_properties if p['id'] not in ids_set]
            ids_set = set([p['id'] for p in properties])

            offers = offers.append({'id': id,
                                    'title': title,
                                    'place': place,
                                    'address': address,
                                    'details': str(details),
                                    'link': link,
                                    'description': desc},
                                   ignore_index=True)

        except Exception as e:
            print(e)
            continue

    properties = pd.DataFrame.from_records(properties)
    return offers, properties


if __name__ == '__main__':
	current_date = str(datetime.now().date())
	gather_new_articles(current_date)