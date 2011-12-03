from datetime import datetime

def parsedate(inputstr, formats):
    for fmt in formats:
        try:
            return datetime.strptime(inputstr, fmt).date()
        except ValueError:
            continue
