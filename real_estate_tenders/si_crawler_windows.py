import bs4
import requests
import re
import os
import pandas as pd
from tqdm import tqdm

short_url = 'http://sales.bcpea.org'


def scrape_si(records, failed):
    records['page_id'] = records['page_id'].astype(int)
    new_page_ids = set(set(records.page_id.values).difference(failed.page_id.values))
    links = records[records.page_id.apply(lambda x: x in new_page_ids)]['link'].values        
    details = pd.DataFrame()

    for link in tqdm(links):
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
        except:
            failed = failed.append(pd.DataFrame({'page_id': [page_id]}), ignore_index=True)

    return details, failed


def main(records):
    failed = pd.read_csv('si_unsuccessful.csv', sep='\t') if os.path.exists('si_unsuccessful.csv') else pd.DataFrame({'page_id':[]})
    details, failed = scrape_si(records, failed)
    records = pd.merge(records, details, on='page_id', how='left')
    # SAVES LOCAL FILES
    failed.to_csv('si_unsuccessful.csv', sep='\t', index=False)

    return records