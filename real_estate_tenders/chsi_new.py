# -*- coding: utf-8 -*-
import boto3
import io
import pandas as pd
import bs4
import requests 
import re
import datetime
import locale
import sqlalchemy as sal
from tqdm import tqdm

_URL = 'https://sales.bcpea.org/properties?perpage=1000&court=28'
_BASE_URL = 'https://sales.bcpea.org'


def get_soup(link):
    resp = requests.get(link)
    return bs4.BeautifulSoup(resp.content , "html.parser")

def main(old_offers):
    page = get_soup(_URL)
    offers = page.findAll('div', attrs={'class':'item__group'})
    df = pd.DataFrame()
    locale.setlocale(locale.LC_ALL, 'bg_BG')
    page.decompose()

    for o in offers:
        try:
            link = o.find('a', href=re.compile('^/properties/[\d]+$'))['href']
            offer_id = int(re.search('/([\d]+)$', link).group(1))

            published = o.find('div', attrs={'class':'date'}).text.replace('Публикувано на ', '').replace(' г. в ', ' ').replace(' часа', '')
            #4 декември 2020 16:31
            published = datetime.datetime.strptime(published, '%d %B %Y %H:%M')

            title = o.find('div', attrs={'class':'title'}).text
            area = float(o.find('div', attrs={'class':'category'}).text.replace(' кв.м', '').replace(' ', '')) if o.find('div', attrs={'class':'category'}) is not None else ''
            price = float(o.find('div', attrs={'class':'price'}).text.replace('лв.', '').replace(' ', ''))
            city = o.find('div', text=re.compile('НАСЕЛЕНО МЯСТО'), attrs={'class':'label'}).parent.findNext('div', attrs={'class':'info'}).text \
                        if o.find('div', text=re.compile('НАСЕЛЕНО МЯСТО'), attrs={'class': 'label'}) is not None \
                        else ''
            address =  o.find('div', text=re.compile('Адрес'), attrs={'class':'label'}).parent.findNext('div', attrs={'class':'info'}).text \
                        if o.find('div', text=re.compile('Адрес'), attrs={'class': 'label'}) is not None \
                        else ''
            organizer = o.find('div', text=re.compile('ЧАСТЕН СЪДЕБЕН ИЗПЪЛНИТЕЛ'), attrs={'class':'label'}).parent.findNext('div', attrs={'class':'info'}).text
            term = o.find('div', text=re.compile('СРОК'), attrs={'class':'label'}).parent.findNext('div', attrs={'class':'info'}).text
            term_start = re.search('от ([\d\.]+)', term).group(1)
            term_end = re.search('до ([\d\.]+)', term).group(1)
            announcement = o.find('div', text=re.compile('ОБЯВЯВАНЕ НА'), attrs={'class':'label'}).parent.findNext('div', attrs={'class':'info'}).text

            data = {'offer_id': [offer_id],
                    'published': [published],
                    'title': [title],
                    'area': [area],
                    'price': [price],
                    'city': [city],
                    'address': [address],
                    'organizer': [organizer],
                    'term_start': [term_start],
                    'term_end': [term_end],
                    'announcement': [announcement],
                    'link': [_BASE_URL + link]}

            df = df.append(pd.DataFrame(data), ignore_index=True)
        except:
            continue
    
    #import pdb; pdb.set_trace()
    df = df[~df['offer_id'].isin(old_offers)]

    return df


def crawl_individually(df):
    df['floor'] = 0
    df['description'] = None
    df['image_count'] = 0

    #import pdb; pdb.set_trace()
    for ind, row in tqdm(df.iterrows()):
        page = get_soup(row['link'])

        floor =  re.search('^([\d]+)', page.find('div', text=re.compile('Етаж'), attrs={'class':'label'}).parent.findNext('div', attrs={'class':'info'}).text).group(1) \
                        if page.find('div', text=re.compile('Етаж'), attrs={'class': 'label'}) is not None \
                        and re.search('^([\d]+)', page.find('div', text=re.compile('Етаж'), attrs={'class':'label'}).parent.findNext('div', attrs={'class':'info'}).text) is not None \
                        else 0

        desc =  page.find('div', text=re.compile('ОПИСАНИЕ'), attrs={'class':'label'}).parent.findNext('div', attrs={'class':'info'}).text.replace('\n', ' ') \
                        if page.find('div', text=re.compile('ОПИСАНИЕ'), attrs={'class': 'label'}) is not None \
                        else ''
        #name, link
        docs = [(re.search('/([^\/]+)$', d.a['href']).group(1), _BASE_URL + d.a['href']) for d in page.find('div', text=re.compile('Сканирани обявления'), attrs={'class':'label'}).parent.findNext('div', attrs={'class':'info'}).ul.findAll('li')]
        images = [(re.search('/([^\/]+)$', i['href']).group(1), _BASE_URL + i['href']) for i in page.findAll('a', attrs={'class':'item-image'}) if i['href'] != '/assets/images/photo-placeholder.png']
        
        page.decompose()

        images_count = len(images)

        df.at[ind, 'floor'] = floor
        df.at[ind, 'description'] = desc
        df.at[ind, 'image_count'] = images_count

        for d in docs:
            save_file(d[0], d[1], row['offer_id'])

        for i in images:
            save_file(i[0], i[1], row['offer_id'])

        
    df.to_csv('df.csv', index=False)

    return df


def save_file(name, link, offer_id):
    session = boto3.session.Session(profile_name='aero')
    s3 = session.resource('s3')
    #download the file
    url = link
    r = requests.get(url)
    if r.status_code == 200:
        s3.Object('bg-tenders', str(offer_id) + '/' + name).put(Body=r.content, ContentType = r.headers['content-type'])


def get_old_offers(engine):
    existing_offers = "select distinct offer_id from tenders"

    return pd.read_sql(existing_offers, engine)['offer_id'].values


def load_in_db(conn, new_offers):
    cur = conn.cursor()
    output = io.StringIO()
    new_offers.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'tenders', null="")
    conn.commit()


if __name__ == '__main__':
    with open('connection_rds.txt', 'r') as f:
        DATABASE_URI = f.read()
        engine = sal.create_engine(DATABASE_URI)
        conn = engine.raw_connection()

    #existing ids
    old_offers = get_old_offers(engine)
    df = main(old_offers)
    new_offers = crawl_individually(df)
    load_in_db(conn, new_offers)