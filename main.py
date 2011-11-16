import os
import sys
from datetime import datetime
from itertools import islice
from multiprocessing import Process, Pipe
from futures import ThreadPoolExecutor, as_completed
from pymongo import Connection

import usaspending
from downloader import (download_file, DownloadFileSuccess, DownloadFileFailure)
from analyzer import analyze_file
from django.conf import settings


#def download_proc_tick(ctl):
#    """Checks for status messages from the downloader process.
#    Returns False if the downloader process has exited. 
#    Returns True otherwise.
#    """
#    if ctl.poll(1):
#        try:
#            obj = ctl.recv()
#            if isinstance(obj, DownloadFileSuccess):
#                print >>sys.stdout, "Downloaded {filename}".format(filename=obj.filename)
#                return (True, obj)
#            elif isinstance(obj, DownloadFileFailure):
#                def cause_clause(cause):
#                    (label, details) = cause
#                    if label == 'HTTP Status':
#                        return "recieved HTTP status {status}".format(status=details)
#                    elif label == 'Exception':
#                        return "unexpected exception {type} {msg}".format(
#                            type=str(type(details)),
#                            msg=str(details))
#                    else:
#                        return "unrecognized error."
#                msg = "Failed to download {filename}, because {cause}".format(
#                    filename=obj.filename,
#                    cause=cause_clause(obj.cause))
#                print >>sys.stdout, msg
#                return (True, obj)
#            else:
#                return (True, None)
#        except EOFError:
#            print >>sys.stdout, "Downloader finished."
#            return (False, None)
#    else:
#        return (True, None)


def store_download_failure(coll, details, message):
    (filename, url, destpath, file_params) = details
    record = coll.find_one(file_params)
    if record is None:
        record = dict(file_params.iteritems())
        record['exceptions'] = []

    record['exceptions'].append({'when': datetime.now(), 
                                 'message': str(message)})
    coll.save(record)


def do_analysis(file_params):
    (filename, url, destpath) = usaspending.file_info(**file_params)
    analysis_fields = settings.ANALYSIS_FIELDS[file_params['spending_type']]
    if analysis_fields:
        analysis = analyze_file(destpath, analysis_fields)
        analysis['filenames'] = [filename]
        analysis.update(file_params)
        return analysis
    else:
        return None

def chunked(seq, size):
    itr = iter(seq)
    while True:
        chunk = list(islice(itr, size))
        if len(chunk) == 0:
            return
        else:
            yield iter(chunk)

def main():
    mongo = Connection()
    delta_db = mongo['benford1'] # TODO: make this a setting
    delta_coll = delta_db['monthly_deltas']
    delta_error_coll = delta_db['delta_errors']

    with ThreadPoolExecutor(max_workers=2) as download_exec:
        for comb_chunk in chunked(usaspending.file_param_combs(), size=3):
            download_details = {}
            for delta_file_params in comb_chunk:
                delta_record = delta_coll.find_one(delta_file_params)
                if not delta_record:
                    (filename, url, destpath) = usaspending.file_info(**delta_file_params)
                    if not os.path.exists(destpath):
                        print >>sys.stdout, "Queueing file for download: {filename}".format(filename=filename)
                        dl = download_exec.submit(download_file, filename, url, destpath)
                        download_details[dl] = (filename, url, destpath, delta_file_params)
                    else:
                        print >>sys.stdout, "Analyzing {filename}".format(filename=filename)
                        analysis = do_analysis(delta_file_params)
                        if analysis:
                            delta_coll.insert(analysis)
                        else:
                            print >>sys.stderr, "Analysis incomplete for {filename}".format(filename=filename)

            for dl in as_completed(download_details):
                dl_details = download_details[dl]
                (filename, url, destpath, file_params) = dl_details

                dl_exception = dl.exception()
                if dl_exception is not None:
                    print >>sys.stderr, "Download failed for {filename} because {cause}.".format(filename=filename,
                                                                                                 cause=str(dl_exception))
                    store_download_failure(delta_error_coll, dl_details, str(dl_exception))
                    continue
                
                dl_result = dl.result()
                if isinstance(dl_result, DownloadFileFailure):
                    print >>sys.stderr, "Download failed for {filename} because {cause}.".format(filename=filename,
                                                                                                 cause=dl_result.cause)
                    store_download_failure(delta_error_coll, dl_details, dl_result.cause)

                elif isinstance(dl_result, DownloadFileSuccess):
                    print >>sys.stderr, "Downloaded {filename}, beginning analysis.".format(filename=filename)
                    analysis = do_analysis(file_params)
                    if analysis:
                        delta_coll.insert(analysis)
                    else:
                        print >>sys.stderr, "Analysis incomplete for {filename}".format(filename=filename)

                else:
                    # WTF?
                    pass
            
            


if __name__ == "__main__":
    main()

