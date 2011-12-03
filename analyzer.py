import sys
import csv
import numpy
from scipy import stats
from decimal import Decimal
from zipfile import ZipFile
from datetime import date
from utils import parsedate
from django.conf import settings

BENFORD = {
    '1': float(0.301),
    '2': float(0.176),
    '3': float(0.125),
    '4': float(0.097),
    '5': float(0.079),
    '6': float(0.067),
    '7': float(0.058),
    '8': float(0.051),
    '9': float(0.046)
}

class InvalidArchive(Exception):
    def __init__(self, *args, **kwargs):
        super(self, Exception).__init__(*args, **kwargs)

def frequencies(values):
    counts = {}
    for value in values:
        count = counts.get(value, 0)
        counts[value] = count + 1
    return counts

def analyze_records(reader, fiscal_year, datefield, fields):
    fy_months = [date(fiscal_year - (1 if month >= 10 else 0),
                      month, 1)
                 for month in range(1, 13)]

    observations = dict(((month, dict.fromkeys(fields, []))
                         for month in fy_months))
    digits = dict(((month, dict.fromkeys(fields, {}))
                   for month in fy_months))

    for record in reader:
        dtstr = record[datefield]
        if dtstr is None or dtstr.strip() == '':
            print >>sys.stderr, "Skipping record with blank date field."
            continue
        dt = parsedate(record[datefield], settings.DATE_FORMATS)
        dt1 = date(dt.year, dt.month, 1)
        if dt1 not in fy_months:
            continue

        obs = observations[dt1]
        digs = observations[dt1]

        for field in fields:
            value = record[field]
            (value, digit) = benford_filter(value)
            if value is not None:
                obs[field].append(value)
            if digit is not None:
                digs[field].append(digit)

    results = dict(((month, dict.fromkeys(fields, {}))
                    for month in fy_months))
    for dt1 in results:
        for field in fields:
            result = results[dt1][field]
            obs = observations[dt1][field]
            obs_array = numpy.array(obs, dtype=float)
            digs = digits[dt1][field]

            result['field_name'] = field
            result['value_count'] = len(obs)
            result['value_sum'] = numpy.sum(obs_array)
            result['mean'] = numpy.mean(obs_array)
            result['median'] = numpy.median(obs_array)
            result['skew'] = stats.skew(obs_array)
            result['digits'] = benford_difference(digs)
            
    return results

def benford_difference(digits):
    """Tallies the frequency of each digit and calculates 
    the percentage therefor. Returns a map of digits to 
    maps of the form {'count', 'percentage'}"""
    result = {}
    one_thru_nine = [str(d) for d in range(1, 10)]
    digit_freqs = frequencies(digits)
    for digit in one_thru_nine:
        count = digit_freqs.get(digit, 0)
        pct = 0 if count == 0 else float(count) / float(len(digits))
        result[digit] = {'count': count, 
                         'percentage': pct, 
                         'difference': pct - BENFORD[digit]}
    return result

def benford_filter(number):
    """Returns a tuple (value, digit). Either member may be None.
    The first member is the value observed for the purpose of 
    calculating a median and mean for the data. The second member
    is the first digit observed if the absolute value of the first
    member is greater than or equal to 1."""
    if number is None:
        return (None, None)

    number = number.strip()
    if len(number) == 0 or number.upper() == 'N/A':
        return (None, None)

    value = Decimal(number)
    if -1 < value < 1:
        return (value, None)

    digit = str(abs(value))[0]
    return (value, digit)

def analyze_file(archive_path, fiscal_year, datefield, fields):
    with ZipFile(archive_path) as archive:
        names = archive.namelist()
        if len(names) == 0:
            raise InvalidArchive("No files inside archive {path}".format(path=archive_path))
        elif len(names) > 1:
            raise InvalidArchive("Multiple files inside archive {path}".format(path=archive_path))
        else:
            # rU provides universal new-line recognition
            with archive.open(names[0], 'rU') as datafile: 
                reader = csv.DictReader(datafile)
                return analyze_records(reader, fiscal_year, datefield, fields)
                
