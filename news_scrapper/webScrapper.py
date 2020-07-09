import boto3
from datetime import datetime, timedelta
from io import StringIO
import logging
import site1
import site2
import site3
import site4
import site5
import site6
import site7
import site8
import site9

def get_new_articles(site):
    articles = None
    
    if site == 'vesti.bg': 
        articles = site1.gather_new_articles('https://www.vesti.bg')
    elif site == 'news.bg':
        articles = site2.gather_new_articles('https://news.bg/')
    elif site == 'blitz.bg':
        articles = site3.gather_new_articles('https://blitz.bg')
    elif site == 'dir.bg':
        articles = site4.gather_new_articles('https://dir.bg/')
    elif site == '24chasa.bg':
        articles = site5.gather_new_articles('https://www.24chasa.bg')
    elif site == 'nova.bg':
        articles = site6.gather_new_articles('https://nova.bg/news')
    elif site == 'fakti.bg': 
        articles = site7.gather_new_articles('https://fakti.bg')
    elif site == 'dnevnik.bg': 
        articles = site8.gather_new_articles('https://www.dnevnik.bg')
    elif site == 'sportal.bg': 
        articles = site9.gather_new_articles('https://www.sportal.bg/')
    
    return articles
    
sites = ['vesti.bg', 'news.bg', 'blitz.bg', 'dir.bg', '24chasa.bg', 'nova.bg', 'fakti.bg', 'dnevnik.bg', 'sportal.bg'] # 


COLUMNS = ['comments', 'views', 'shares', 'created_timestamp', 'visited_timestamp',
           'tags', 'section', 'title', 'subtitle', 'category', 'link',
           'article_text', 'author', 'thumbs_down', 'thumbs_up', 'location']

DESTINATION_BUCKET = 'news-scrapping'


def save_file():
    for site in sites:
        logging.debug('Scrapping {}'.format(site))
        # not UTC but EET
        now = datetime.now()
        now_date = str(now.date())
        now_hour = str(now.hour)
        file_name = site + '_' + now_date + '_' + now_hour + 'h.tsv'

        articles = get_new_articles(site)
        articles.rename(columns={'date': 'created_timestamp'}, inplace=True)
        articles['visited_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # accomodate all columns across all datasets
        for col in COLUMNS:
            if col not in articles.columns:
                articles[col] = None

        articles = articles[COLUMNS]

        csv_buffer = StringIO()
        articles.to_csv(csv_buffer, sep='\t', encoding='utf-16', index=False)
        logging.debug(site + ' has ' + str(articles.shape[0]) + ' articles.\n')
        s3 = boto3.resource('s3')
        s3.Object(DESTINATION_BUCKET, 'raw/' + site + '/' + now_date + '/' + file_name).put(Body=csv_buffer.getvalue())


if __name__ == '__main__':
    save_file()
