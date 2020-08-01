import os

l = ['address', 'arco', 'etuovi', 'holmes', 'imoteka', 'superimoti', 'vuokraovi', 'yavlena']

for li in l:
    os.system('aws s3 rm s3://real-estate-scrapping/raw/{}/2020-07-17/ --profile aero --recursive'.format(li))
    os.system('aws s3 rm s3://real-estate-scrapping/raw/{}/2020-07-18/ --profile aero --recursive'.format(li))
    #os.system('aws s3 cp s3://real-estate-scrapping/raw/{}/2020-07-17/{}_2020-07-17.tsv s3://real-estate-scrapping/raw/{}/2020-07-16/{}_2020-07-16.tsv --profile aero'.format(li, li, li, li))
    #os.system('aws s3 rm s3://real-estate-scrapping/raw/{}/2020-07-17/{}_2020-07-17.tsv --profile aero'.format(li, li))

