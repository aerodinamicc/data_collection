import requests as rq
import bs4
import pandas as pd
import os
import re

mitmproxy_string = 'https://www.google.com/search?vet='

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

centers_file = 'shopping_centers.csv'
centers = pd.DataFrame()

if os.path.isfile(centers_file):
    stores = pd.read_csv(centers_file)
else:
    stores = pd.DataFrame(columns={'center_id', 'address', 'name', 'reviews', 'link', 'lon', 'lat'})


exists = os.path.isfile('poi_sites.txt')

sites = open('malls.txt', 'r')
sites_list = sites.read().split('\n')

for url in sites_list:
    print(sites_list.index(url))
    response = rq.get(url)
    soup = bs4.BeautifulSoup(response.content, 'lxml')

    places = soup.select('.rllt__mi')

    meta = soup.select('.rl-qs-crs-t')

    for ind in range(1, len(places)):
        center_id = places[ind]['data-id']
        lon = places[ind]['data-lng']
        lat = places[ind]['data-lat']
        name = places[ind].find_all('div')[4].text
        review = 0
        if len(places[ind].find_all('div')) > 6:
            review = places[ind].find_all('div')[6].text[1:-1]  # The number is placed between brackets

        #print('%s %s %s %s %s' % (center_id, lon, lat, name, review))

        link = meta[ind].find_all('a')
        if len(link) > 1:
            link = link[1]['href']
        else:
            link = ''

        rating = meta[ind].select('.BTtC6e')
        if len(rating) > 0:
            rating = rating[0].text
        else:
            rating = ''

        address_from_div = ''
        try:
            address_from_div = re.search('<span>(\d+(-\d+)?\s.+?)<\/span>', str(meta[ind])).group(1)
        except:
            try:
                address_from_div = re.search('<span>(\w+,\s.+)<\/span>', str(meta[ind])).group(1)
            except:
                address_from_div = ''
                
        # The address is not so straight forward as it could only be acquired for the places which have a google maps link associated with them
        address_from_link = ''
        maps = "https://maps.google.com"
        if link.startswith(maps):
            start_index = link.find('daddr=') + len('daddr=')
            end_index = link.find('&', start_index)
            address_from_link = link[start_index:end_index].replace('+', ' ')
        else:
            address_from_link = meta[ind].find_all('span')
            if len(address_from_link) > 3:
                address_from_link = address_from_link[3].text

        centers = centers.append({'center_id': center_id,
                                  'address_from_link': address_from_link,
                                  'address_from_div': address_from_div,
                                  'name': name,
                                  'reviews': review,
                                  'rating': rating,
                                  'link': link,
                                  'lon': lon,
                                  'lat': lat},
                                  ignore_index=True)

        #number = 0
        #for div in meta[ind].find_all('span'):
        #    # The address div changes sometimes
        #    number = number + 1
        #    print(number)
        #    print(div)
        #number = 0
centers.to_csv('shopping_malls.csv')

print('end')
