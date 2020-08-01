import bs4
import os
import requests
import pandas as pd
import re
from tqdm import tqdm
from datetime import datetime
from helpers import clean_text


base_url = 'https://www.vuokraovi.com/'
search_url = 'https://www.vuokraovi.com/vuokra-asunnot/{}?page={}'
cities = ['Espoo', 'Helsinki', 'Vantaa']
offers_file = "vuokraovi_"


def get_page_count(page):
    max_page = max([int(re.search('([\d]+)', li.text).group(1)) for li in page.find('ul', attrs={'class':'pagination'}).findAll('li') if re.search('[\d]+', li.text) is not None])
    return max_page


def gather_new_articles():
    offers = crawlLinks() 
    offers['is_for_sale'] = False

    return offers


def crawlLinks():
    offers = pd.DataFrame()
    for city in cities:
        resp = requests.get(search_url.format(city, str(1)))
        page = bs4.BeautifulSoup(resp.text, features='html.parser')
        page_count = get_page_count(page)

        for page_n in tqdm(range(1, page_count + 1)):
            resp = requests.get(search_url.format(city, str(page_n)))
            page = bs4.BeautifulSoup(resp.text, features='html.parser')

            boxes = page.findAll('div', attrs={'class': 'list-item-container'})
            for b in boxes:
                try:
                    link = b.find('a', attrs={'class': 'list-item-link'})['href']
                    # rental-apartment/espoo/suurpelto/block+of+flats/722129?entryPoint=fromSearch&rentalIndex=1
                    id = re.search('([\d]+?)\?', link).group(1) if re.search('([\d]+?)\?', link) is not None else ''
                    available_from =  clean_text(b.find('span', attrs={'class': 'showing-lease-container'}).li.text) if len(b.find('span', attrs={'class': 'showing-lease-container'}).findAll('li')) > 0  else ''
                    address =  clean_text(b.find('span', attrs={'class': 'address'}).text) if len(b.findAll('span', attrs={'class': 'address'})) > 0 else ''

                    meta = b.find('ul', attrs={'class': 'list-unstyled'})
                    price = clean_text(meta.find('span', attrs={'class': 'price'}).text) if len(b.findAll('span', attrs={'class': 'price'})) > 0 else '0'
                    price = re.search('([\d ]+)(?:[\d,]+)? €\/kk$', price).group(1).replace(' ', '') if re.search('([\d ]+)(?:[\d,]+)? €\/kk$', price) is not None else '0'
                    typ_and_area = meta.find('li').text if len(meta.findAll('li')) > 0 else ''
                    typ = typ_and_area.split(',')[0].strip() if len(typ_and_area) > 0 else ''
                    area = typ_and_area.split(',')[1].replace('m²', '').strip() if len(typ_and_area) > 0 else ''
                    details = meta.findAll('li')[1].text.strip() if len(meta.findAll('li')) > 1 else ''
                    '''
                    company = b.find('div', attrs={'class': 'hidden-xs col-sm-3 col-4'}).a.img['alt'] if \
                        len(b.findAll('div', attrs={'class': 'hidden-xs col-sm-3 col-4'})) > 0 \
                        and len(b.find('div', attrs={'class': 'hidden-xs col-sm-3 col-4'}).findAll('a')) > 0 else ''
                    '''

                    offers = offers.append({'link': base_url + link[1:],
                                            'id': id,
                                            'available_from': available_from,
                                            'details': details,
                                            'type': typ,
                                            'city': city,
                                            'place': address,
                                            'price': price,
                                            #'company': company,
                                            'area': area}, ignore_index=True)

                except Exception as e:
                    print(e)
                    continue

    return offers


if __name__ == '__main__':
	gather_new_articles()