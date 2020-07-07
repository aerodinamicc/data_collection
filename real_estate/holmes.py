#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import bs4
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


def get_neighbourhood_links():
    search_page_template = '/pcgi/home.cgi?act=5&f1=0&f2=1&f3=0&f4=%E3%F0%E0%E4%20%D1%EE%F4%E8%FF'
    # page_link = base_url + search_page_template
    resp = requests.get(base_url)
    page = bs4.BeautifulSoup(resp.content.decode('cp1251'), 'html')
    neighbourhoods = page.findAll('a',
                                  href=re.compile('http://sofia\.holmes\.bg/pcgi/home\.cgi.*f4=град(?:\s)?София.*f5=[А-Яа-я0-9\s]+'),
                                  # text=re.compile('[\d]+'),
                                  attrs={'class': 'linkSearch'})
    neighbourhoods = [re.search('&f5=(.*)$', i['href']).group(1) for i in neighbourhoods]
    neighbourhoods = list(set([(n, base_url + search_page_template + '&f5=' + urllib.parse.quote(n.encode('cp1251'))) for n in neighbourhoods]))
    return neighbourhoods


def get_all_links_for_each_neighbourhood(neighbourhoods, current_date):
    links = set([])

    for n in neighbourhoods:
        pages = set([1])
        last_page = False
        page_link = n[1]

        while not last_page:
            resp = requests.get(page_link)
            page = bs4.BeautifulSoup(resp.content.decode('cp1251'), 'html')

            visible_pages = page.findAll('a', href=re.compile('http://sofia\.holmes\.bg/pcgi/home\.cgi.*f6='), text=re.compile('[\d]+'), attrs={'class': 'pageNumbers'})
            visible_pages = [int(i.text) for i in visible_pages]

            next_page = page.findAll('a', href=re.compile('http://sofia\.holmes\.bg/pcgi/home\.cgi.*f6='), text=re.compile('^(?![\d]+)'), attrs={'class': 'pageNumbers'})
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
            visible_links = page.findAll('a', href=re.compile('/pcgi/home\.cgi.*act=3.*adv=[\w\d]+$'), attrs={'class': 'linkLocatList'})
            visible_links = [(base_url + i['href'], n[0]) for i in visible_links]
            links.update(set(visible_links))

        print('{}: {}'.format(n[0], len(search_pages)))

    links = list(set(links))
    links_df = pd.DataFrame(links, columns=['link', 'neighbourhood'])
    if not os.path.exists('output'):
        os.mkdir('output')
    links_df.to_csv('output/' + links_file + current_date + '.tsv', sep='\t', index=False)

    return links_df['link'].values,  links_df['neighbourhood'].values


def gather_new_articles(current_date):
    if os.path.exists(links_file + current_date + '.tsv'):
        links = list(pd.read_csv(links_file + current_date + '.tsv', sep='\t')['link'].values)
        nbbhds = list(pd.read_csv(links_file + current_date + '.tsv', sep='\t')['neighbourhood'].values)
    else:
        neighbourhoods = get_neighbourhood_links()
        links, nbbhds = get_all_links_for_each_neighbourhood(neighbourhoods, current_date)

    offers = crawlLinks(links, nbbhds, current_date)
    offers = offers[['link', 'title', 'address', 'details', 'neighbourhood', 'lon', 'lat', 'id', 'price', 'price_sqm', 'area', 'floor', 'description', 'views', 'date', 'agency', 'poly']]											   
    offers.to_csv('output/' + offers_file + current_date + '.tsv', sep='\t', index=False)

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

    return clean_string(desc)


def get_date(page):
    date_start_phrase = 'Публикувана в '
    date_end_phrase = '<br/>'
    date_start_ind = str(page).find(date_start_phrase) + len(date_start_phrase)
    date_end_ind = str(page).find(date_end_phrase, date_start_ind)
    date = str(page)[date_start_ind:date_end_ind] if date_start_ind > 100 else ''
    articleDate = date
    month_name = [m for m in months.keys() if m in articleDate][0]
    # "15:54 на 21 февруари, 2020 год."
    articleDate = articleDate.replace(month_name,
                                      replace_month_with_digit(month_name)) if month_name is not None else articleDate
    articleDate = pd.to_datetime(articleDate, format='%H:%M на %d %m, %Y год.')

    return articleDate


def get_details(keys, values):
    dict = {}
    if len(keys) != len(values):
        return None
    else:
        for i in range(len(keys)):
            dict[keys[i].text] = values[i].text

        return json.dumps(dict, ensure_ascii=False)


def clean_string(str):
    return str.replace('\n', ' ').replace('\t', ' ').strip()



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

            id = re.search('=([\d\w]+)$', link).group(1)
            lon = page.findAll('input', attrs={'name': 'mapn', 'type': 'hidden'})[0]['value'].split(',')[0] \
                if len(page.findAll('input', attrs={'name': 'mapn', 'type': 'hidden'})) > 0 \
                else ''
            lat = page.findAll('input', attrs={'name': 'mapn', 'type': 'hidden'})[0]['value'].split(',')[1] \
                if len(page.findAll('input', attrs={'name': 'mapn', 'type': 'hidden'})) > 0 \
                else ''

            address = clean_string(page.findAll('td', attrs={'width': '225'})[0].text) \
                if len(page.findAll('td', attrs={'width': '225'})) > 0 \
                else ''
            poly = clean_string(page.findAll('input', attrs={'name': 'p', 'type': 'hidden'})[0]['value']) \
                if len(page.findAll('input', attrs={'name': 'p', 'type': 'hidden'})) > 0 \
                else ''
            details_key = page.findAll('td', attrs={'width': '100'})
            details_value = page.findAll('td', attrs={'width': '230'})
            details = get_details(details_key, details_value)

            price = page.find_all('span', {'id': re.compile('^cena$')})[0].text
            price_sq = page.find_all('span', {'id': re.compile('^cenakv')})[0].text
            agency = get_agency(page)

            views = page.find(text=re.compile('Обявата е посетена'))
            views = views.find_next_sibling().text if views is not None else ''
            date = get_date(page)
            desc = get_desc(page)
            area = page.find('td', text=re.compile('Квадратура:'))
            area = area.find_next_sibling().text if area is not None else ''
            floor = page.find('td', text=re.compile('Етаж:'))
            floor = floor.find_next_sibling().text if floor is not None else ''

            title = page.findAll('input', attrs={'name': 'f0', 'type': 'hidden'})
            title = title[0]['value'].split(',')[0] if len(title) > 0 else ''

            current_offer = pd.DataFrame(data={'link': link,
                                               'title': title,
                                               'address': address,
                                               'details': details,
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