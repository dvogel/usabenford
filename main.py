import os
import sys
import json
from functools import partial
from datetime import datetime, date
from itertools import islice
from futures import ThreadPoolExecutor, as_completed
from pymongo import Connection

import usaspending
from downloader import (download_file, DownloadFileSuccess, DownloadFileFailure)
from analyzer import analyze_file
from django.conf import settings

def chunked(seq, size):
    itr = iter(seq)
    while True:
        chunk = list(islice(itr, size))
        if len(chunk) == 0:
            return
        else:
            yield iter(chunk)

def analyses_needed(analyses_found):
    months_found = set([a['month'] for a in analyses_found])
    months_needed = set(range(1, 13)) - months_found
    return len(months_needed) > 0

def download_and_analyze(fiscal_year, agency, spending_type):
    """The is a worker function, run on a separate thread."""
    dbconn = Connection()
    db = dbconn[settings.DB_NAME]
    monthly_analyses = db['monthly_analyses']

    analyses = monthly_analyses.find({'fiscal_year': fiscal_year, 'agency': agency, 'spending_type': spending_type})
    needed = analyses_needed(analyses)
    if needed:
        (filename, url, destpath) = usaspending.file_info(fiscal_year, agency, spending_type)
        dl_result = download_file(filename, url, destpath)
        if isinstance(dl_result, DownloadFileFailure):
            return (False, dl_result)
        print >>sys.stdout, "Got file %s" % filename
        
        analyses = analyze_file(destpath, fiscal_year, 
                                settings.ANALYSIS_DATEFIELDS[spending_type],
                                settings.ANALYSIS_FIELDS[spending_type])
        save_analyses(db, fiscal_year, agency, spending_type, analyses)
        return (True, analyses)

    return (True, None)
   
def save_analyses(db, fiscal_year, agency, spending_type, analyses):
    monthly_analyses = db['monthly_analyses']

    for (dt1, field_analyses) in analyses.items():
        for (field_name, analysis) in field_analyses.items():
            analysis['year'] = dt1.year
            analysis['month'] = dt1.month
            analysis['fiscal_year'] = fiscal_year
            analysis['agency'] = agency
            analysis['spending_type'] = spending_type
            monthly_analyses.save(analysis)

def main():
    for fiscal_year in settings.FISCAL_YEARS:
        timewarp = ThreadPoolExecutor(max_workers=3)
        results = timewarp.map(lambda combs: apply(download_and_analyze, combs),
                               usaspending.file_param_combs(fiscal_year))
        for result in results:
            success = result[0]
            if success:
                analyses = result[1]
                if analyses:
                    for dt1, field_analyses in analyses.items():
                        for field_name, analysis in field_analyses.items():
                            print "Analysis completed for {fy}, {m}/{y}, {a}, {st}, {fld}".format(
                                fy=analysis['fiscal_year'],
                                m=analysis['month'],
                                y=analysis['year'],
                                a=analysis['agency'],
                                st=analysis['spending_type'],
                                fld=analysis['field_name'])
            else:
                error = result[1]
                if isinstance(error, DownloadFileFailure):
                    print >>sys.stderr, "Failed to download %s because %s" % (error.filename, error.cause)
                else:
                    print >>sys.stderr, str(error)


if __name__ == "__main__":
    main()

