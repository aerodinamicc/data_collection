import bs4
import os
import requests
import pandas as pd
import re
from tqdm import tqdm
from datetime import datetime



base_url = 'https://imoteka.bg'
search_url = "https://imoteka.bg/{}/sofiya?locations=4451&page={}"
offers_file = "imoteka_"

def get_page_count(page):
    max_page = max([int(re.search('([\d]+)', a.text).group(1)) for a in page.findAll('a', attrs={'class':'page-link'}) if re.search('[\d]+', a.text) is not None])
    return max_page


def gather_new_articles(current_date):
    #resp_sale = requests.get(search_url.format('sale', '1'))
    #page_sale = bs4.BeautifulSoup(resp_sale.text, 'html')
    #resp_rent = requests.get(search_url.format('rent', '1'))
    #page_rent = bs4.BeautifulSoup(resp_rent.text, 'html')

    with open('C:/Users/shadow/Downloads/imoteka_search_p1.html', 'r', encoding='utf-8') as f:
        resp_sale = f.read()

    with open('C:/Users/shadow/Downloads/imoteka_result_rent.html', 'r', encoding='utf-8') as f:
        resp_rent = f.read()

    page_sale = bs4.BeautifulSoup(resp_sale, 'html')
    page_rent = bs4.BeautifulSoup(resp_rent, 'html')

    page_count_sale = get_page_count(page_sale)
    page_count_rent = get_page_count(page_rent)

    offers_sale = crawlLinks('sale', 1) #page_count_sale)  
    offers_rent = crawlLinks('rent', 1) #page_count_rent)    
    offers = pd.concat([offers_rent, offers_sale], ignore_index=True)       

    offers = offers[['link', 'id', 'type', 'is_for_sale', 'place', 'views', 'over_under', 'price', 'currency' ,'area', 'labels']]
    offers['is_for_sale'] = offers['is_for_sale'].astype(bool)										   
    if not os.path.exists('output'):
        os.mkdir('output')
    offers.to_csv('output/' + offers_file + current_date + '.tsv', sep='\t', index=False)

    return offers


def crawlLinks(type_of_offering, page_count):
    offers = pd.DataFrame()

    for page_n in tqdm(range(1, page_count + 1)):
        #resp = requests.get(search_url.format(type_of_offering, str(page_n)))
        #page = bs4.BeautifulSoup(resp.text, 'html')
        with open('C:/Users/shadow/Downloads/imoteka_search_p1.html' if type_of_offering == 'sale' \
                else 'C:/Users/shadow/Downloads/imoteka_result_rent.html', 'r', encoding='utf-8') as f:
            resp = f.read()

        page = bs4.BeautifulSoup(resp, 'html')

        boxes = page.findAll('div', attrs={'class': 'list__item'})

        for b in boxes:
            try:
                id = re.search('([\d]+)', b.select('.list__img-id')[0].text).group(1)
                meta = b.findAll('div', attrs={'class': 'list__info-container'})[0].findAll('div', attrs={'class': 'list__info'})[0].findAll('div')
                nbhd =  meta[1].text
                area = meta[2].text.replace('Квадратура: ', '').replace('M2', '').strip()
                price = meta[3].text.replace('Цена: ', '')
                if 'EUR' in price:
                    price = price.replace('EUR', '').replace(' ', '')
                    currency = 'EUR'
                elif 'BGN' in price:
                    price = str(round(float(price.replace('BGN', '').replace(' ', '')) / 1.9558))
                    currency = 'BGN'

                typ = b.findAll('span', attrs={'class':re.compile('truncate-label')})[0].text
                is_for_sale = typ.split('/')[0].strip() == 'Продажба'
                typ = '/'.join(typ.split('/')[1:])
                link = b.select('.list__info-container')[0].findAll('a', text=re.compile('Вижте в детайли'))[0]['href']
                labels = ', '.join([l['data-tooltip'] for l in b.findAll('div', attrs={'data-tooltip': re.compile('.*')})])
                views = ''.join([d.text for d in b.findAll('g')[0].findAll('text')])
                over_under = b.findAll('div', attrs={'data-tooltip': re.compile('Спрямо всички оферти в района')})[0].findAll('div', attrs={'class':re.compile('list__price-text')})[0].text 

                offers = offers.append({'link': link,
                                        'id': id,
                                        'is_for_sale': is_for_sale,
                                        'currency': currency,
                                        'type': typ,
                                        'labels': labels,
                                        'views': views,
                                        'over_under': over_under,
                                        'place': nbhd,
                                        'price': price,
                                        'area': area}, ignore_index=True)

            except Exception as e:
                print(e)
                continue

    return offers


if __name__ == '__main__':
	current_date = str(datetime.now().date())
	gather_new_articles(current_date)