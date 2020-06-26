import bs4
import requests
import re
import pandas as pd
import os
from tqdm import tqdm
import si_crawler_windows as details_crawler
import map_places_to_locations as mp


site = 'http://sales.bcpea.org/bg/properties-p{}.html?sorter=8'
short_url = 'http://sales.bcpea.org'


def get_page_count():
    request = requests.get(site.format(1))
    soup = bs4.BeautifulSoup(request.text, 'html.parser')
    pages_count = soup.select('#ctl0_Content_Pager')[0].select('span')[0].select('a')
    pages_count = re.search('.*properties-p(\d+)\..*', pages_count[-1]['href']).group(1)

    return pages_count


def scrape(exisiting_records):
    records = pd.DataFrame(columns=['price', 'type', 'link', 'area', 'place', 'court_short', 'court_full',
                                    'notarius', 'period_start', 'period_end', 'announcement', 'published'])
    existing_page_ids = set(exisiting_records.page_id.values) if 'page_id' in exisiting_records.columns else set()
    unsuccessful = []
    pages_count = get_page_count()

    for page in tqdm(range(1, int(pages_count)+1)):
        request = requests.get(site.format(page))
        soup = bs4.BeautifulSoup(request.text, 'lxml')

        results = soup.select('.results_list')[0].select('li')

        for ind in range(0, len(results)):
            result = results[ind]
            link = short_url + result.select('h2')[0].select('a')[0]['href']
            page_id = int(re.search('.*item(\d+)\..*', link).group(1))

            if page_id in existing_page_ids:
                records = format_dates(records)
                return records, pd.DataFrame(data={'link': unsuccessful})

            try:
                price = result.select('.start_price')[0].text.replace('\r\n', ' ').replace('\n', ' ').replace('\t', ' ')
                price = re.search('.*:\s+(\d+)\..*', price).group(1)
                type = result.select('h2')[0].select('a')[0].text
                area_and_place = result.select('p')[0].text.replace('\r\n', ' ').replace('\n', '-').replace('\t', ' ')
                area = re.search('^(?:[\s-]+)?(\d+)\s+кв\.м\..*', area_and_place).group(1)
                place = re.search('^(?:[\s-]+)?\d+\s+кв\.м\.,(.*)', area_and_place).group(1).strip()
                place = re.sub('\\s+', ' ', place).replace('-', ',').rstrip(',').rstrip()
                meta = result.select('p')[1].text.replace('\r\n', ' ').replace('\n', ' ').replace('\t', ' ')
                if 'Софийски окръжен съд' in meta:
                    court_short = 'София-област'
                elif 'Софийски градски съд' in meta:
                    court_short = 'София-град'
                else:
                    court_short = re.search('.*Окръжен\sсъд\s+([а-яА-Я]+(?:\sЗагора|\sТърново)?).*', meta).group(1)

                court_full = re.search('.*Окръжен\s+съд:\s+([а-яА-Я ]+).*', meta).group(1).replace(' ЧСИ', '').strip()
                notarius = re.search('.*ЧСИ:\s+([а-яА-Я -]+).*', meta).group(1).replace(' Срок', '').strip()
                period_start = re.search('.*\sот\s([\d\.]+).*', meta).group(1).strip()
                period_end = re.search('.*\sдо\s([\d\.]+).*', meta).group(1).strip()
                announcement = re.search('.*Обявяване\sна:\s+([\d\.\s:]+).*', meta).group(1).strip()
                published = re.search('.*Публикуванa\sна:\s+([\w\.\s:]+)', result.select('.date_created')[0].text
                                       .replace('\r\n', ' ').replace('\n', ' ').replace('\t', ' ')).group(1).strip()

                records = records.append({'link': link,
                                          'page_id': page_id,
                                          'type': type,
                                          'price': price,
                                          'area': area,
                                          'place': place,
                                          'court_short': court_short,
                                          'court_full': court_full,
                                          'notarius': notarius,
                                          'period_start': period_start,
                                          'period_end': period_end,
                                          'announcement': announcement,
                                          'published': published.strip()},
                                          ignore_index=True)
            except requests.exceptions.ConnectionError as e:
                return format_dates(records), pd.DataFrame(data={'link': unsuccessful})
            except:
                unsuccessful.append(link)

    records = format_dates(records)

    return records, pd.DataFrame(data={'link': unsuccessful})


def format_dates(records):
    records['period_start'] = pd.to_datetime(records['period_start'], format='%d.%m.%Y')
    records['period_end'] = pd.to_datetime(records['period_end'], format='%d.%m.%Y')
    records['announcement'] = pd.to_datetime(records['announcement'], format='%d.%m.%Y %H:%M')
    records['published'] = pd.to_datetime(records['published'], format='%d.%m.%Y %H:%M')

    return records


def main():
    # READS LOCAL FILES
    records = pd.read_csv('si_rec.csv', sep='\t') if os.path.exists('si_rec.csv') else pd.DataFrame()
    rec, failed = scrape(records)
    rec = details_crawler.main(rec)
    rec = mp.match_place_names_to_location(rec)

    rec = pd.concat([rec, records], sort=False)
    rec[['page_id', 'area', 'place', 'price',  'type', 'court_full', 'court_short', 'link', 'published', 'period_end',
       'period_start', 'announcement', 'notarius', 'description', 'additional_desc', 'ideal_parts', 'city_clean', 'mun_clean']]\
        .to_csv('si_rec.csv', sep='\t', index=False)


if __name__ == '__main__':
    main()
