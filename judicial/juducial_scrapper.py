import bs4
import os
import requests
import pandas as pd
import re
from tqdm import tqdm
from datetime import datetime


base_url = 'https://burgas-os.justice.bg/bg/2955?from=&to=&actkindcode=&casenumber=354&caseyear=2018&casetype='

def clean_text(text):
    return text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('\\xa0', ' ').replace('\xa0', ' ').replace('"', "'").replace('„', "'").replace('“', "'"). replace('&nbsp;', '').strip()


def loop_links():
    resp = requests.get(base_url)
    page = bs4.BeautifulSoup(resp.text)

    opr = page.findAll('tr',attrs={'class':'table-data'})
    links_dates = []
    
    import pdb; pdb.set_trace()
    for o in opr:
        lnk = o.find('a', text='Свали')
        date = o.find('td', text=re.compile('\d{2}\.\d{2}\.\d{4}'))
        if lnk is not None:
            links.append((lnk['href'], date.text))

    for l, d in links_dates:
        resp = requests.get(l)
        page = bs4.BeautifulSoup(resp.text)
        text = get_text(page)

        if 'Здрашко Танев Алексиев' in text:
            with open('file{}'.format(d), 'w') as f:
                f.write(resp.text)



def get_text(page):
    par =  page.findAll('p', attrs={'class': 'MsoNormal'})
    return [clean_text(p.text) for p in par if clean_text(p.text) != '']


if __name__ == '__main__':
    loop_links()
