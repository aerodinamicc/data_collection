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


def gather_new_articles(current_date):
    offers = crawlLinks() 
    offers = offers[['link', 'id', 'type', 'details', 'available_from', 'city', 'place', 'price', 'area', 'company']]
    import pdb; pdb.set_trace()
									   
    if not os.path.exists('output'):
        os.mkdir('output')
    offers.to_csv('output/' + offers_file + current_date + '.tsv', sep='\t', index=False)

    return offers


def crawlLinks():
    offers = pd.DataFrame()
    for city in cities:
        #resp = requests.get(search_url.format(city, str(1)))
        #page = bs4.BeautifulSoup(resp.text, 'html')
        #page_count = get_page_count(page_sale)
        with open('C:/Users/shadow/Downloads/vuokraovi_results.html', 'r', encoding='utf-8') as f:
                resp = f.read()

        page = bs4.BeautifulSoup(resp, 'html')

        page_count = get_page_count(page)

        for page_n in tqdm(range(1, 2)): # page_count + 1)):
            #resp = requests.get(search_url.format(type_of_offering, str(page_n)))
            #page = bs4.BeautifulSoup(resp.text, 'html')
            with open('C:/Users/shadow/Downloads/vuokraovi_results.html', 'r', encoding='utf-8') as f:
                resp = f.read()

            page = bs4.BeautifulSoup(resp, 'html')

            boxes = page.findAll('div', attrs={'class': 'list-item-container'})
            for b in boxes:
                try:
                    link = b.find('a', attrs={'class': 'list-item-link'})['href']
                    # rental-apartment/espoo/suurpelto/block+of+flats/722129?entryPoint=fromSearch&rentalIndex=1
                    id = re.search('([\d]+?)\?', link).group(1) if re.search('([\d]+?)\?', link) is not None else ''
                    available_from =  clean_text(b.find('span', attrs={'class': 'showing-lease-container'}).li.text)
                    address =  clean_text(b.find('span', attrs={'class': 'address'}).text)

                    meta = b.find('ul', attrs={'class': 'list-unstyled'})
                    price = clean_text(meta.find('span', attrs={'class': 'price'}).text)
                    typ_and_area = meta.findAll('li')[0].text
                    typ = typ_and_area.split(',')[0].strip()
                    area = typ_and_area.split(',')[1].replace('m²', '').strip()
                    details = meta.findAll('li')[1].text.strip()
                    company = b.find('div', attrs={'class': 'hidden-xs col-sm-3 col-4'}).a.img['alt'] if \
                        len(b.findAll('div', attrs={'class': 'hidden-xs col-sm-3 col-4'})) > 0 else ''

                    offers = offers.append({'link': link,
                                            'id': id,
                                            'available_from': available_from,
                                            'details': details,
                                            'type': typ,
                                            'city': city,
                                            'place': address,
                                            'price': price,
                                            'area': area,
                                            'company': company}, ignore_index=True)

                except Exception as e:
                    print(e)
                    continue

    return offers


if __name__ == '__main__':
	current_date = str(datetime.now().date())
	gather_new_articles(current_date)