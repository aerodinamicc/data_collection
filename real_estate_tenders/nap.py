import bs4
import requests
import re
import pandas as pd
import os
from tqdm import tqdm
import nap_crawler as npc

site = 'https://sales.nra.bg/realestate?p={}'
short_url = 'https://sales.nra.bg'


def get_page_count():
    request = requests.get(site.format(1))
    soup = bs4.BeautifulSoup(request.text, 'lxml')
    pages_count = soup.select('.pages')[0].select('li')
    pages_count = pages_count[-1].select('a')[0]['href']
    pages_count = re.search('.*p=(\d+).*', pages_count).group(1)

    return pages_count


def scrape_nra(exisiting_records):
    records = pd.DataFrame(columns=['price', 'type', 'link', 'desc', 'place', 'period_end', 'announcement', 'bidding', 'page_id'])
    existing_page_ids = set(exisiting_records['page_id'].values)
    unsuccessful = []

    pages_count = get_page_count()
    for page in tqdm(range(1, int(pages_count)+1)):
        request = requests.get(site.format(page))
        soup = bs4.BeautifulSoup(request.text, 'lxml')

        results = soup.select('.singlePublication')

        for result in results:
            try:
                sections = result.select('.col-sm-12')
                link = short_url + sections[2].select('div')[1].select('p')[1].select('a')[0]['href'].strip()
                page_id = int(re.search('.*#item(.*)', link).group(1))

                if page_id in existing_page_ids:
                    continue

                desc = sections[1].select('div')[1].text.replace('\n', ' ').strip()
                type = sections[2].select('div')[1].select('p')[0].text.replace('\n', ' ').strip()
                place = sections[3].select('div')[1].select('p')[0].text.replace('\n', ' ').strip()
                bidding = sections[4].select('div')[1].select('p')[0].text.replace('\n', ' ').strip()
                price = re.search('(\d+)(?:\.)?.*', sections[5].select('div')[1].select('p')[0].text.strip()).group(1)

                dates = sections[6].select('div')[1].select('p')
                period_end = dates[1].select('span')[0].text.replace('\n', ' ').strip()
                announcement = dates[2].select('span')[0].text.replace('\n', ' ').strip()


                records = records.append({'link': link,
                                          'page_id': page_id,
                                          'type': type,
                                          'price': price,
                                          'desc': desc,
                                          'place': place,
                                          'bidding': bidding,
                                          'period_end': period_end,
                                          'announcement': announcement},
                                          ignore_index=True)
            except:
                unsuccessful.append(link)
    records = format_dates(records)
    return records, pd.DataFrame(data={'link': unsuccessful})


def format_dates(records):
    records['period_end'] = pd.to_datetime(records['period_end'], format='%d.%m.%Y')
    records['announcement'] = pd.to_datetime(records['announcement'], format='%d.%m.%Y')

    return records


def main():
    records = pd.read_csv('nap_rec.csv', sep='\t') if os.path.exists('nap_rec.csv') else pd.DataFrame({'page_id':[]})
    new_rec, failed = scrape_nra(records)
    new_page_ids = set(set(new_rec['page_id'].astype(int).values).difference(records['page_id'].astype(int).values))
    # 'page_id', 'link', 'related_page_ids', 'period_start', 'period_end', 'announcement', 'published', 'category',
    # 'ideal_part', 'desc', 'quantity', 'already_held', 'deposit', 'price', 'responsible_person', 'department',
    # 'region', 'municipality', 'place', 'address', 'type', 'area', 'photo_links', 'lat', 'lon'
    new_links = new_rec[new_rec['page_id'].astype(int).isin(new_page_ids)]['link']
    records_details = npc.main(new_links)
    records_details = pd.concat([records_details, records], sort=True)
    records_details.to_csv('nap_rec.csv', sep='\t', index=False)


if __name__ == '__main__':
    main()
