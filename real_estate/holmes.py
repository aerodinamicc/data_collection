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

            visible_pages = page.findAll('a', href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f6='), text=re.compile('[\d]+'))
            visible_pages = [int(i.text) for i in visible_pages]

            next_page = page.findAll('a', href=re.compile('sofia\.holmes\.bg/pcgi/home\.cgi.*f6='), text=re.compile('^(?![\d]+)'))
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

            visible_links = page.findAll('a', href=re.compile('/pcgi/home\.cgi.*act=3.*adv=[\w\d]+$'))
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
    links, nbbhds = get_all_links_for_each_neighbourhood(neighbourhoods[:2], current_date)

    offers = crawlLinks(links, nbbhds, current_date)
    offers = offers[['link', 'title', 'address', 'details', 'neighbourhood', 'lon', 'lat', 'id', 'price', 'price_sqm', 'area', 'floor', 'description', 'views', 'date', 'agency', 'poly']]	
    csv_buffer = StringIO()
    offers.to_csv(csv_buffer, sep='\t', encoding='utf-16', index=False)

    s3 = session.resource('s3')
    s3.Object(DESTINATION_BUCKET, 'raw/' + site + '/' + now_date + '/' + file_name).put(Body=csv_buffer.getvalue())

    return offers


def get_agency(page):
    agency_start_phrase = 'Агенция:<br/>\n<b>'
    agency_end_phrase = '</b>'
    agency_start_ind = str(page).find(agency_start_phrase) + len(agency_start_phrase)
    agency_end_ind = str(page).find(agency_end_phrase, agency_start_ind)
    agency = str(page)[agency_start_ind:agency_end_ind] if len(str(page)[agency_start_ind:agency_end_ind]) < 100 else ''

    return agency


def get_desc(page):
    desc_start_phrase = '<b>Допълнителна информация:</b><br/>'
    desc_end_phrase = '<'
    desc_start_ind = str(page).find(desc_start_phrase) + len(desc_start_phrase)
    desc_end_ind = str(page).find(desc_end_phrase, desc_start_ind)
    desc = str(page)[desc_start_ind:desc_end_ind] if desc_start_ind > 100 else ''

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
	gather_new_articles(current_date)