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


mapping = {
    'vesti.bg': site1.gather_new_articles('https://www.vesti.bg'),
    'news.bg': site2.gather_new_articles('https://news.bg/'),
    'blitz.bg': site3.gather_new_articles('https://blitz.bg'),
    'dir.bg': site4.gather_new_articles('https://dir.bg/'),
    '24chasa.bg': site5.gather_new_articles('https://www.24chasa.bg'),
    'nova.bg': site6.gather_new_articles('https://nova.bg/news'),
    'fakti.bg': site7.gather_new_articles('https://fakti.bg'),
    'dnevnik.bg': site8.gather_new_articles('https://www.dnevnik.bg'),
    'sportal.bg': site9.gather_new_articles('https://www.sportal.bg/')
}


COLUMNS = ['comments', 'views', 'shares', 'created_timestamp', 'visited_timestamp',
           'tags', 'section', 'title', 'subtitle', 'category', 'link',
           'article_text', 'author', 'thumbs_down', 'thumbs_up', 'location']

DESTINATION_BUCKET = 'news-scrapper'


def save_file(event, context):
    for site in mapping.keys():
        # not UTC but EET
        now = datetime.now() + timedelta(hours=2)
        now_date = str(now.date())
        now_hour = str(now.hour)
        file_name = site + '_' + now_date + '_' + now_hour + 'h.tsv'

        articles = mapping[site]
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

