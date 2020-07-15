import bs4
import os
import requests
import pandas as pd
import re
from tqdm import tqdm
from datetime import datetime
from helpers import clean_text

#https://www.superimoti.bg/bulgaria/selski-kashti/

base_url = 'https://superimoti.bg'
search_url = "https://www.superimoti.bg/search/index.php?&country_id=1&stown=0&sregion=25&maxprice_curr=NaN&spredlog=innear&action=1&csel=&searchform=1&sadx={}&hmsf=1&page={}"
offers_file = "superimoti_"

def get_page_count(page):
    max_page = max([int(re.search('([\d]+)', a.text).group(1)) for a in page.findAll('a', attrs={'href': re.compile('&page=')}) if re.search('[\d]+', a.text) is not None])
    return max_page


def gather_new_articles():
    current_date = str(datetime.now().date())
    resp_sale = requests.get(search_url.format('1', '1'))
    page_sale = bs4.BeautifulSoup(resp_sale.text, 'html')

    resp_rent = requests.get(search_url.format('2', '1'))
    page_rent = bs4.BeautifulSoup(resp_rent.text, 'html')

    page_count_sale = get_page_count(page_sale)
    page_count_rent = get_page_count(page_rent)

    offers_sale = crawlLinks('sale', page_count_sale)
    offers_rent = crawlLinks('rent', page_count_rent)
    offers = pd.concat([offers_rent, offers_sale], ignore_index=True)       
    offers['is_for_sale'] = offers['is_for_sale'].astype(bool)
    
    return offers


def crawlLinks(type_of_offering, page_count):
    offers = pd.DataFrame()

    for page_n in tqdm(range(1, page_count + 1)):
        resp = requests.get(search_url.format(1 if type_of_offering == 'sale' else 2, str(page_n)))
        page = bs4.BeautifulSoup(resp.text, 'html')

        boxes = page.findAll('div', attrs={'class': 'offer'})
        for b in boxes:
            try:
                link = b.find('a', attrs={'class':'lnk'})['href']
                title = b.find('a', attrs={'class':'lnk'})['title'].replace(' - SUPER ИМОТИ', '')
                label = clean_text(b.find('div', attrs={'class':'band'}).text) if len(b.findAll('div', attrs={'class':'band'})) > 0 else ''
                typ = clean_text(b.find('div', attrs={'class':'ttl'}).text)
                id = b['id'].replace('prop', '')
                place = clean_text(b.find('div', attrs={'class':'loc'}).text).replace('Софийска област, България', '').replace('гр. София / ', '').replace('кв. ', '')
                is_for_sale = type_of_offering == 'sale'
                
                #Details
                keys = b.find('div', attrs={'class':'lst'}).findAll('b')
                values = b.find('div', attrs={'class':'lst'}).findAll('i')
                details = {}
                for i in range(len(keys)):  
                    details[keys[i].text.strip()] = values[i].text.strip()

                #total_floors = details['Етажност на сградата:'] if 'Етажност на сградата:' in details.keys() else ''

                #Extending labels and cleaning the 'type' field
                if 'В процес на строителство' in typ:
                    typ = typ.replace('В процес на строителство', '').replace('(', '').replace(')', '').strip()
                    label = label + ', В процес на строителство' if len(label) > 0 else 'В процес на строителство'
                if 'апартаменти' in typ.lower():
                    label = label + ', Множество апартаменти' if len(label) > 0 else 'Множество апартаменти'
                if 'Предварителни продажби' in typ:
                    typ = typ.replace('Предварителни продажби', '').replace('(', '').replace(')', '').strip()
                    label = label + ', Предварителни продажби' if len(label) > 0 else 'Предварителни продажби'
 
                area = details['Площ:'].replace(' м²', '') if 'Площ:' in details.keys() \
                    else details['Площ на сградата:'].replace(' м²', '') if 'Площ на сградата:' in details.keys() \
                    else details['Площи:'].replace(' м²', '') if 'Площи:' in details.keys() \
                    else ''
                area = area if len(area) > 0 else '0'

                #floor = details['Етаж:'] if 'Етаж:' in details.keys() and '/' not in details['Етаж:'] \
                #    else details['Етаж:'].split('/')[0] if 'Етаж:' in details.keys() else ''

                #Price
                price = clean_text(b.find('div', attrs={'class':'prc'}).text).replace('\xa0', '')
                price = re.search('Наем€ ([\d\s]+)/месец', price).group(1).replace(' ', '') if re.search('Наем€ ([\d\s]+)/месец', price) is not None and type_of_offering == 'rent' and len(price) > 0 and '–' not in price \
                    else re.search('€([\d\s]+)', price).group(1).replace(' ', '') if re.search('€([\d\s]+)', price) is not None and type_of_offering == 'sale' and len(price) > 0 and '–' not in price \
                    else '0'
                
                if 'без ДДС' in price:
                    clean_price = int(clean_price) + int(clean_price)*0.2
                
                offers = offers.append({'link': link,
                                        'label': label,
                                        'type': typ,
                                        'is_for_sale': is_for_sale,
                                        'title': title,
                                        'id': id,
                                        'details': str(details),
                                        'place': place,
                                        'price': price,
                                        'area': area}, ignore_index=True)

            except Exception as e:
                print(e)
                continue

    return offers


if __name__ == '__main__':
	gather_new_articles()