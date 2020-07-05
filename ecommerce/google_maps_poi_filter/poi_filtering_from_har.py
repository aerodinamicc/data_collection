import json
from haralyzer import HarParser, HarPage
import os

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

gsearch = 'https://www.google.com/search?vet'
links = []

for i in range(1,5):
    mall = 'malls' + str(i) + '.har'
    with open(mall, 'r', encoding="utf8") as f:
        text = f.read()

        start_index = text.find(gsearch)

        while(start_index != -1):
            end_index = text.find('",', start_index)
            url = text[start_index:end_index]
            links.append(url)
            start_index = text.find(gsearch, end_index)

result = "\n".join(links)

with open('malls.txt', 'w', encoding='utf8') as f:
    f.write(result)







