#!/usr/bin/env python
# -*- coding: utf-8 -*-
import bs4
import requests
import pandas as pd
from urllib.parse import urlsplit
from urllib.parse import unquote
import re
from tqdm import tqdm
from helpers import clean_text, replace_month_with_digit

def gather_new_articles(site):
    request = requests.get(site)
    soup = bs4.BeautifulSoup(request.text, features="html.parser")

    all_articles = set([site + unquote(a['href']) for a in soup.find_all('a', href=True)
                        if a['href'].startswith('news.php?news=')])

    all_articles = crawlLinks(all_articles)

    return all_articles


def crawlLinks(links):
    articles_content = pd.DataFrame()

    for link in tqdm(links):
        try:
            rq = requests.get(link)
            domain = "{0.netloc}".format(urlsplit(link))

            if rq.status_code == 200:
                page = bs4.BeautifulSoup(rq.text, features="html.parser")

                category = page.find('div', attrs={'class': 'printing_large_text_toolbar'}).text if page.find('div', attrs={'class': 'printing_large_text_toolbar'}) is not None else ''
                headline = page.select('#news_heading')[0].h1.text.strip() if len(page.select('#news_heading')) > 0 else ''
                shares = page.select(".social_count")[0].text.strip() if len(page.select(".social_count")) > 0 else ''
                comments = page.select('.comments')[0].text.strip() if len(page.select('.comments')) else ''
                views = page.select('.btn_reads')[0].text.split('Прочетена')[1].strip() if len(page.select('.btn_reads')) > 0 else ''
                article_text = clean_text(page.select('#news_content')[0].text) if len(page.select('#news_content')) > 0 else ''

                # 01 януари 2020 | 16:26 - Обновена
                articleDate = page.find('td', attrs={'id': 'news_heading'})
                articleDate = articleDate.find('span', attrs={'class': 'dark_text'}).text if articleDate is not None and articleDate.find('span', attrs={'class': 'dark_text'}) is not None else ''
                articleDate = articleDate.split('- Обновена')[0].strip() if articleDate != '' else ''
                if articleDate != '':
                    month_name = re.search('([а-яА-Я]+)', articleDate)
                    month_name = month_name.group(1) if month_name is not None else None
                    articleDate = articleDate.replace(month_name, replace_month_with_digit(
                        month_name)) if month_name is not None else articleDate
                    articleDate = pd.to_datetime(articleDate, format='%d %m %Y | %H:%M')

                author = page.select('#author_box')[0].select('h5')[0].a.text if len(page.select('#author_box')) > 0 else ''
                tags = " - ".join([clean_text(i.text) for i in page.find('div', attrs={'class': 'news_tags'}).findAll('span')])
                
                articles_content = articles_content.append({'link': link,
                                                            'title': clean_text(headline),
                                                            'author': clean_text(author),
                                                            'date': articleDate,
                                                            'category': clean_text(category),
                                                            'tags': tags,
                                                            'comments': clean_text(comments),
                                                            'views': clean_text(views),
                                                            'shares': clean_text(shares),
                                                            'article_text': article_text},
                                                           ignore_index=True)
        except:
            continue

    return articles_content
