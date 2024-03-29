import bs4
import requests
import re
from datetime import datetime, date
import pandas as pd
from tqdm import tqdm
import urllib.parse
import ast


short_url = 'https://sales.nra.bg'


def scraping(links):
    details = pd.DataFrame()

    for link in tqdm(links):
        request = requests.get(link)
        soup = bs4.BeautifulSoup(request.text, 'lxml')

        published = soup.find("span", text="Дата на съобщение:")
        published = re.search('^(?:\s)?([\d\.]+).*', published.nextSibling).group(1).strip() if published else ''

        period_start = soup.find("span", text='Начална дата за подаване на предложения:')
        period_start = re.search('^(?:\s)?([\d\.]+).*', period_start.nextSibling).group(1).strip() if period_start else ''

        period_end = soup.find("span", text="Краен срок за внасяне на депозит:")
        period_end = re.search('^(?:\s)?([\d\.]+).*', period_end.nextSibling).group(1).strip() if period_end else ''

        announcement = soup.find("span", text='Дата  и час на провеждане на търга:')
        announcement = re.search('^(?:\s)?([\d\.\s:]+).*', announcement.nextSibling).group(1).strip() if announcement else ''

        responsible_person = soup.find("span", text='Водещ продажбата:')
        responsible_person = responsible_person.nextSibling.strip() if responsible_person else ''

        department = soup.find("span", text='ТД на НАП:')
        department = department.nextSibling.strip() if department else ''

        deposit = soup.find("span", text='Депозит:')
        deposit = re.search('^([\d\.\s]+).*', deposit.nextSibling).group(1).strip() if deposit else ''

        price = soup.find("span", text='Първоначална цена:')
        price = re.search('^([\d\.\s]+).*', price.nextSibling).group(1).strip() if price else ''

        already_held = soup.find("span", text='Брой провеждания:')
        already_held = re.search('^(?:\s)?([\d]+).*', already_held.nextSibling).group(1).strip() if already_held else ''

        tenders = soup.select('.tender-item')
        page_ids =[]
        for i in tenders:
            page_ids.append(re.search('.*item(.*)', i['id']).group(1))

        for i in tenders:
            page_id = re.search('.*item(.*)', i['id']).group(1)
            related_page_ids = list(set(page_ids) - set([page_id]))

            category = soup.find("span", text='Категория:')
            category = category.nextSibling.strip() if category else ''

            quantity = soup.find("span", text='Количество:')
            quantity = re.search('^(?:[\s\n]+)?([\d]+).*', quantity.nextSibling).group(1).strip() if quantity else ''

            region = soup.find("span", text='Област:')
            region = region.nextSibling.strip() if region else ''

            municipality = soup.find("span", text='Община:')
            municipality = municipality.nextSibling.strip() if municipality else ''

            place = soup.find("span", text='Населено място:')
            place = place.nextSibling.strip() if place else ''

            address = soup.find("span", text='Адрес:')
            address = address.nextSibling.strip() if address else ''

            desc = soup.find("span", text='Описание:')
            desc = desc.nextSibling.strip() if desc else ''

            length = 50
            x = desc
            ideal_parts = re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]).group(1) \
                if str(x) not in ['nan', ''] \
                   and re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]) is not None \
                   and re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]).group(
                1) is not None \
                else re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]).group(2) \
                if str(x) not in ['nan', ''] \
                   and re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]) is not None \
                   and re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]).group(2) \
                else None

            area = soup.find("span", text='Квадратура:')
            area = area.nextSibling.strip() if area else ''

            type = soup.find("span", text='Вид недвижим имот:')
            type = type.nextSibling.strip() if type else ''

            photo_links = []
            photos = i.select('.carousel-inner')[0].select('div')
            for p in photos:
                p_link = p.select('a')
                p_link = short_url + p_link[0].img['src'] if len(p_link) > 0 else ''
                if not p_link:
                    continue
                p_link = urllib.parse.unquote(urllib.parse.unquote(p_link))
                p_link = re.search('(.*)\?.*', p_link).group(1)
                photo_links.append(p_link)

            coor = i.select('.map')
            lat = round(ast.literal_eval(coor[0]['data-coordinates'])['lat'], 5) if len(coor) > 0 else ''
            lng = round(ast.literal_eval(coor[0]['data-coordinates'])['lng'], 5) if len(coor) > 0 else ''

            details = details.append({'page_id': page_id,
                                      'link': link,
                                      'related_page_ids': related_page_ids,
                                      'period_start': period_start,
                                      'period_end': period_end,
                                      'announcement': announcement,
                                      'published': published,
                                      'category': category,
                                      'ideal_part': ideal_parts,
                                      'desc': desc,
                                      'quantity': quantity,
                                      'already_held': already_held,
                                      'deposit': deposit,
                                      'price': price,
                                      'responsible_person': responsible_person,
                                      'department': department,
                                      'region': region,
                                      'municipality': municipality,
                                      'place': place,
                                      'address': address,
                                      'type': type,
                                      'area': area,
                                      'photo_links': photo_links,
                                      'lat': lat,
                                      'lon': lng},
                                     ignore_index=True)

        # to_date fails
        # addition_to_records['period_start'] = pd.to_datetime(addition_to_records['period_start'], format='%d.%m.%Y')
        # addition_to_records['period_end'] = pd.to_datetime(addition_to_records['period_end'], format='%d.%m.%Y')
        # addition_to_records['announcement'] = pd.to_datetime(addition_to_records['announcement'], format='%d.%m.%Y %H:%M')
        # addition_to_records['published'] = pd.to_datetime(addition_to_records['published'], format='%d.%m.%Y %H:%M')

    return details


def main(records):
    records = scraping(records)
    return records


if __name__ == '__main__':
    main()
