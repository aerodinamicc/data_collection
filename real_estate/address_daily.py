import bs4
import os
import requests
import pandas as pd
import re
from tqdm import tqdm
from datetime import datetime
from helpers import clean_text


search_url = 'https://address.bg/bg/продажба/резултати-от-търсене.html?page={}&city=101&etype=4&etype=3&etype=5&etype=6&etype=15&etype=2&etype=10&etype=12&etype=17&etype=18&pricefrom=&priceto=&areafrom=&areato=&sortBy=1&offerno=&region=1062&region=1063&region=1064&region=1065&region=1066&region=1067&region=1068&region=1069&region=1070&region=1071&region=1072&region=1074&region=1077&region=1078&region=1079&region=1080&region=1081&region=1082&region=1083&region=1084&region=1085&region=1086&region=1087&region=1088&region=1089&region=1090&region=1091&region=1092&region=1093&region=1094&region=1095&region=1096&region=1097&region=1098&region=1099&region=1100&region=1101&region=1102&region=1103&region=1104&region=1105&region=1106&region=1107&region=1108&region=1109&region=1110&region=1111&region=1112&region=1113&region=1114&region=1115&region=1116&region=1117&region=1118&region=1119&region=1120&region=1121&region=1122&region=1123&region=1124&region=1125&region=1126&region=1127&region=1128&region=1129&region=1130&region=1131&region=1132&region=1133&region=1134&region=1135&region=1136&region=1137&region=1138&region=1139&region=1140&region=1141&region=1142&region=1143&region=1144&region=1145&region=1146&region=1147&region=1148&region=1149&region=1150&region=1151&region=1152&region=1153&region=1154&region=1155&region=1156&region=1157&region=1158&region=1159&region=1160&region=1161&region=1162&region=1163&region=1164&region=1165&region=1166&region=1167&region=1168&region=1169&region=1170&region=1171&region=1172&region=1173&region=1174&region=1175&region=1176&region=1177&region=1178&region=1179&region=1180&region=1182&region=1183&region=1184&region=1185&region=1186&region=1189&region=1224&region=1225&region=1226&region=1227&region=1228&region=1229&region=1230&region=1231&region=1232&region=1233&region=1234&region=1235&region=1236&region=1290&region=1291&region=1308&region=1319&region=1320&region=1321&region=1322&region=1323&region=1324&region=1325&region=1326&region=1327&region=1328&region=1424&region=1425&region=1426&region=1525&region=1623&region=1659&region=1680&region=1681&region=1682&region=1683&region=1687&floorfrom=&floorto='
offers_file = "address_"

def get_page_count(page):
    max_page = max([int(re.search('([\d]+)', a.text).group(1)) for a in page.find('div', attrs={'class': 'pagination'}).findAll('a') if re.search('[\d]+', a.text) is not None])
    x = page.find('div', attrs={'class': 'pagination'}).findAll('a')
    #import pdb; pdb.set_trace() 

    return max_page


def gather_new_articles():
    resp_sale = requests.get(search_url.format('1'))
    page_sale = bs4.BeautifulSoup(resp_sale.text, 'html')
    page_count_sale = get_page_count(page_sale)
    
    offers = crawlLinks(page_count_sale)
    offers['is_for_sale'] = True

    return offers


def crawlLinks(page_count):
    offers = pd.DataFrame()

    for page_n in tqdm(range(1, page_count + 1)):
        resp = requests.get(search_url.format(str(page_n)))
        page = bs4.BeautifulSoup(resp.text, 'html')
        boxes = page.findAll('div', attrs={'class': 'property_holder'})

        for b in boxes:
            try:
                id = b.findAll('input', attrs={'id': 'estateId'})[0]['value']

                link = clean_text(b.findAll('a', attrs={'class': 'detail'})[0]['href'])
                link =  re.search('^(.*)\?', link).group(1) if re.search('^(.*)\?', link) is not None else ''
                city =  b.findAll('input', attrs={'id': 'cityName'})[0]['value']
                nbhd =  b.findAll('input', attrs={'id': 'quarterName'})[0]['value']
                typ = b.findAll('img', attrs={'class': 'estate_image'})[0]['alt']
                labels = ', '.join([l['alt'] for l in b.findAll('div', attrs={'class': 'estate-labels'})[0].findAll('img')])
                desc = b.findAll('div', attrs={'class': 'description'})[0].text
                broker_info = b.findAll('div', attrs={'class': 'broker-info'})[0].text

                price = b.findAll('input', attrs={'id': 'formattedPrice'})[0]['value']
                if 'EUR' in price:
                    price = price.replace('EUR', '').replace(' ', '')
                    currency = 'EUR'
                elif 'BGN' in price:
                    price = str(round(float(price.replace('BGN', '').replace(' ', '')) / 1.9558))
                    currency = 'EUR'
                
                offers = offers.append({'link': clean_text(link),
                                        'id': id,
                                        'type': clean_text(typ),
                                        'labels': clean_text(labels),
                                        'city': clean_text(city),
                                        'place': clean_text(nbhd),
                                        'price': clean_text(price),
                                        'currency': clean_text(currency),
                                        'broker_info': clean_text(broker_info),
                                        'description': clean_text(desc)}, ignore_index=True)

            except Exception as e:
                print(e)
                continue

    return offers


if __name__ == '__main__':
	gather_new_articles()