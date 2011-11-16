import os
from datetime import date
from django.conf import settings


def file_info(agency, spending_type, year, month, day):
    fmt = '{year}_{agency}_{spending_type}_Delta_{year}{month!s:0>2}{day!s:0>2}.csv.zip'
    filename = fmt.format(agency=agency,
                          spending_type=spending_type,
                          year=year,
                          month=month,
                          day=day)
    destpath = os.path.join(settings.DELTA_DEST_DIR, filename)
    url = 'http://usaspending.gov/datafeeds/' + filename
    return filename, url, destpath


def file_param_combs():
    today = date.today()
    for year in range(today.year, 1999, -1):
        for agency in settings.AGENCIES:
            for spending_type in settings.SPENDING_TYPES:
                for month in range(1, 13):
                    if year < today.year or (year == today.year and month <= today.month):
                        details = { 
                            'agency': agency,
                            'spending_type': spending_type,
                            'year': year,
                            'month': month,
                            'day': 1 }
                        yield details


