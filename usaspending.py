import os
from datetime import date
from django.conf import settings


def fiscal_year_months(year):
    for month in [10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
        yield date(year, month, 1)


def file_info(fiscal_year, agency, spending_type):
    today = date.today()
    fmt = '{fiscal_year}_{agency}_{spending_type}_Full_{year}{month!s:0>2}{day!s:0>2}.zip'
    filename = fmt.format(fiscal_year=fiscal_year,
                          agency=agency,
                          spending_type=spending_type,
                          year=today.year,
                          month=today.month if today.day >= 15 else today.month - 1,
                          day='01')
    destpath = os.path.join(settings.DOWNLOAD_DEST_DIR, filename)
    url = 'http://usaspending.gov/datafeeds/' + filename
    return filename, url, destpath


def file_param_combs(fiscal_year):
    for agency in settings.AGENCIES:
        for spending_type in settings.SPENDING_TYPES:
            yield (fiscal_year, agency, spending_type)


