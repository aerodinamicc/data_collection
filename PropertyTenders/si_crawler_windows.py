import lxml
import bs4
import requests
import re
from datetime import datetime, date
import pandas as pd
from tqdm import tqdm
import urllib.request
import os

short_url = 'http://sales.bcpea.org'


def download_files(resource, page_id):
    if not os.path.exists('output/si/' + page_id):
        os.mkdir('output/si/' + page_id)
        myfile = requests.get(resource)
        open('output/si/' + page_id + '/' + page_id + '.pdf', 'wb').write(myfile.content)


def scrape_si():
    records = pd.read_csv('si_records.csv', sep='\t')
    records['page_id'] = records['page_id'].astype(int)
    details = pd.read_csv('si_details.csv', sep='\t') if os.path.exists('si_details.csv') else pd.DataFrame(columns=['page_id', 'description', 'additional_desc'])
    details['page_id'] = details['page_id'].astype(int)
    new_page_ids = set(records.page_id.values).difference(set(details.page_id.values))
    links = records[records.page_id.apply(lambda x: x in new_page_ids)]['link'].values

    for link in tqdm(links, total=len(links)):
        page_id = int(re.search('.*item(\d+)\..*', link).group(1))
        request = requests.get(link)
        soup = bs4.BeautifulSoup(request.text, 'lxml')
        try:
            desc_text = ''
            ideal_parts = ''
            desc_dict = ''
            desc = soup.select('.offer_description')
            if len(desc) > 0:
                desc_text = desc[0].select('p')[0].text.replace('\r\n', ' ').replace('\n', ' ').replace('\t', ' ')

                desc_dict = {}
                if len(desc) > 1:
                    for ind in range(1, len(desc)):
                        title = desc[ind].select('h2')[0].text
                        elements = desc[ind].select('li')

                        elements_list = []
                        for el in elements:
                            elements_list.append(el.text)
                        desc_dict[title] = elements_list
                length = 50
                x = desc_text
                ideal_parts = re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]).group(1) \
                                        if str(x) not in ['nan', ''] \
                                        and re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]) is not None \
                                        and re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]).group(1) is not None \
                                        else re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]).group(2) \
                                        if str(x) not in ['nan', ''] \
                                        and re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]) is not None \
                                        and re.search('(.*)(?:ид|ИД|Ид)(?:\.|еалн|ЕАЛН)|^\s*([\d]+[/\\\][\d]+)', x[:length]).group(2) \
                                        else None

            details = details.append({'page_id': page_id,
                                      'description': desc_text,
                                      'additional_desc': str(desc_dict),
                                      'ideal_parts': ideal_parts},
                                      ignore_index=True)

            resource = short_url + "/" + soup.select('.offer_details')[0].select('p')[2].select('a')[0]['href']
            download_files(resource, str(page_id))
        except:
            print('Error')

    return details


def main():
    if not os.path.exists('output'):
        os.mkdir('output')
    if not os.path.exists('output/si'):
        os.mkdir('output/si')

    df = scrape_si()
    df.to_csv('si_details.csv', sep='\t', index=False)


if __name__ == '__main__':
    main()