months = {
    'януари': '01',
    'февруари': '02',
    'март': '03',
    'април': '04',
    'май': '05',
    'юни': '06',
    'юли': '07',
    'август': '08',
    'септември': '09',
    'октомври': '10',
    'ноември': '11',
    'декември': '12'
}


def clean_text(text):
    return text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('\\xa0', ' ').replace('"', "'").replace('„', "'").replace('“', "'").strip()


def replace_month_with_digit(month_name):
    for key in months.keys():
        if key.startswith(month_name.lower()):
            return months[key]
