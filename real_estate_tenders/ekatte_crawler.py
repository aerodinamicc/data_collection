import bs4
import re
import pandas as pd
import os
from selenium import webdriver
import multiprocess as mp
import urllib.parse


site = 'https://www.ekatte.com'


def get_coordinates(site):
    from selenium import webdriver
    import re
    import bs4

    browser = webdriver.Chrome(executable_path='.\webdriver\chromedriver.exe')
    browser.get(site)
    text = browser.page_source
    browser.quit()

    lon_phrase = '"longitude":'
    lat_phrase = '"latitude":'
    lon_index = text.find(lon_phrase)
    lat_index = text.find(lat_phrase)
    lon = re.search('([\d\.]+)', text[lon_index:]).group(1)
    lat = re.search('([\d\.]+)', text[lat_index:]).group(1)

    name = bs4.BeautifulSoup(text, 'lxml')
    name = name.select('head')[0]
    name = name.select('title')[0].text

    place_name = name.split(' - ')[0]
    rest = name.split(' - ')[1]
    region = rest.split(', ')[0]
    mun = rest.split(', ')[1]
    ekatte = rest.split(', ЕКАТТЕ ')[1]

    mun = mun.replace('община ', '')
    region = region.replace('област ', '')

    mun = 'София-град' if mun == 'Столична' else mun
    region = 'София-област' if region == 'София (столица)' or region == 'София' else region

    return site, place_name, mun, region, lon, lat, ekatte


def get_list(site):
    driver = webdriver.Chrome(executable_path='.\webdriver\chromedriver.exe')
    driver.get(site)
    soup = bs4.BeautifulSoup(driver.page_source, 'lxml')
    driver.close()

    plc_list = []
    for href in soup.select('.views-row'):
        plc = site + href.select('a')[0]['href']
        plc_list.append(plc)

    return plc_list


def main():
    if not os.path.exists('regions.csv'):
        reg_links = set()
        regions = get_list(site)
        for reg in regions:
            reg_links.add(site + reg.select('a')[0]['href'])

        places = set()
        for reg in reg_links:
            driver = webdriver.Chrome(executable_path='.\webdriver\chromedriver.exe')
            driver.get(reg)
            soup = bs4.BeautifulSoup(driver.page_source, 'lxml')
            driver.close()

            if len(soup.select('.pager')) > 0:
                # multi
                lnks = get_list(reg)
                places.update(lnks)
                lis = soup.select('.pager')[0].select('li')

                for li in lis[1:-2]:
                    li_link = site + li.select('a')[0]['href']
                    lnks = get_list(li_link)
                    places.update(lnks)

            else:
                # single
                lnks = get_list(reg)
                places.update(lnks)

        out_df = pd.DataFrame({'lnks': list(places)})
        out_df.to_csv('regions.csv', sep='\t', index=False)
    else:
        places = list(pd.read_csv('regions.csv', sep='\t')['lnks'].values)

    if os.path.exists('places.csv'):
        places_done = pd.read_csv('places.csv', sep='\t')
        done = set(places_done['link'].values)
        places = set(places) - done
    else:
        places_done = pd.DataFrame()

    pool = mp.Pool(mp.cpu_count() - 1)
    results = pool.map(get_coordinates, places)
    places_df = pd.DataFrame(results, columns=['link',  'place_name', 'mun', 'region', 'lon', 'lat', 'ekatte'])
    #places_df = places_df['link'].apply(lambda x: urllib.parse.unquote(x))
    places_df = pd.concat([places_done, places_df])

    places_df.to_csv('places.csv', sep='\t', index=False)


if __name__ == '__main__':
    main()