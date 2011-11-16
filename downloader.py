import os
import time
import httplib2
import httplib
from django.conf import settings

assert os.path.isdir(settings.DELTA_DEST_DIR)

class ShutdownCmd():
    pass

class DownloadFileCmd():
    def __init__(self, filename, url, destpath):
        self.filename = filename
        self.url = url
        self.destpath = destpath

class DownloadFileSuccess():
    def __init__(self, filename):
        self.filename = filename

class DownloadFileFailure():
    def __init__(self, filename, cause):
        self.filename = filename
        self.cause = cause

def download_file(filename, url, destpath):
    if os.path.isfile(destpath):
        return DownloadFileSuccess(filename)
    else:
        http = httplib2.Http(cache=settings.CACHE_DIR)
        response, content = http.request(url)
        if int(response['status']) == httplib.OK:
            with file(destpath, 'w') as outf:
                outf.write(content)
                return DownloadFileSuccess(filename)
        else:
            return DownloadFileFailure(filename, ('HTTP Status', response['status']))


def download_files(pipe):
    while True:
        if pipe.poll(1):
            try:
                cmd = pipe.recv()
                try:
                    if isinstance(cmd, DownloadFileCmd):
                        outcome = download_file(cmd.filename, cmd.url, cmd.destpath)
                        pipe.send(outcome)
                    elif isinstance(cmd, ShutdownCmd):
                        return
                    else:
                        time.sleep(1)
                        continue
                except Exception, e:
                    pipe.send(DownloadFileFailure(cmd.filename, ('Exception', e)))
            except EOFError:
                return


