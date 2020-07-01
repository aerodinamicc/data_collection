import argparse
import bs4
import requests
import pandas as pd
import re
from tqdm import tqdm
from helpers import clean_text, replace_month_with_digit, months
import time
import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


base_url = 'https://yavlena.bg'
base_search_url = "drundni_{}_maini"
offers_file = "yavlena.bg_"


def get_neighbourhood_links():
    rq = request.get(base_search_url)
    page = bs4.BeautifulSoup(rq, 'html')
    neighbourhoods  = page.findAll('input',
                                  attrs={'name': 'quarters',
                                        'type':'checkbox'})

    #tuple(neighbourhood_name, link)
    neighbourhoods = [(n['data-quarter'], base_search_url.format(n['value'])) for n in neighbourhoods]
    
    return neighbourhoods


def gather_new_articles(current_date):
    neighbourhoods = get_neighbourhood_links()

    offers = crawlLinks(neighbourhoods)            
    offers = offers[['link', 'type', 'extras', 'place', 'lon', 'lat', 'price', 'area', 'description']]											   
    offers.to_csv(offers_file + current_date + '.tsv', sep='\t', index=False)

    return offers


def crawlLinks(neighbourhoods):
    offers = pd.DataFrame()
    browser = webdriver.Chrome(executable_path='./webdriver/chromedriver.exe')
	
    for nbhd, nbhd_link in tqdm(neighbourhoods):
        browser.get(nbhd_link)
        time.sleep(10)
        page = bs4.BeautifulSoup(browser.page_source, 'html')
        load_more = False
        
        while load_more:
            # 1
            button = WebDriverWait(browser, 20).until(EC.element_to_be_clickable((By.ID, "btnNew")))
            button.click()

            # 2
            browser.find_element_by_xpath("//div[@class='vhodOptions']//input[@value=2]").click()

            # 3
            browser.execute_script()

            # click Зареди Още
            time.sleep(2)
            page = bs4.BeautifulSoup(browser.page_source, 'html')
            load_more = len(page.findAll('a', text = 'Зареди още', attrs={'class': 'load-more-results-list'})) > 0

        # scrape all offer boxes for that neighbourhood
        boxes = page.findAll('article', attrs={'class': 'card-list-item'})

        for b in boxes:
            try:
                price = b.select('.price-label')[0].text.replace('&nbsp;', '').replace('\xa0', '') if len(b.select('.price-label')) > 0 else 0
                desc = b.select('.full-text')[0].text if len(b.select('.full-text')) > 0 else 0
                tbody = b.findAll('tbody')[0]
                is_selling = tbody.findAll('tr')[0].findAll('td')[0].h3.text
                type_ = tbody.findAll('tr')[0].findAll('td')[1].h3.text
                link = tbody.findAll('tr')[0].findAll('td')[1].h3.a['href']
                lat = tbody.findAll('tr')[1].findAll('td')[0].a['data-map-lat']
                lng = tbody.findAll('tr')[1].findAll('td')[0].a['data-map-lng']
                area = tbody.findAll('tr')[2].findAll('td')[0].text.replace('кв.м.', '').strip()

                extras = {}
                exs = b.select('.extras')[0]
                exs = exs.select('div')
                for ex in exs:
                    spans = ex.select('span')

                    if len(spans) == 1:
                        extras[spans[0]['title']] = 0
                    elif len(spans) == 2:
                        extras[spans[1]['title']] = spans[0].text

                offers = offers.append({'link': link,
                                        'is_selling': is_selling,
                                        'type': type_,
                                        'extras': str(extras),
                                        'place': nbhd,
                                        'lon': lng,
                                        'lat': lat,
                                        'price': price,
                                        'area': area,
                                        'description': desc}, ignore_index=True)

            except Exception as e:
                print(e)
                continue
    
    browser.close()

    return offers


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-current_date', required=True, help="MMDD")
	parsed = parser.parse_args()
	current_date = parsed.current_date
	gather_new_articles(current_date)