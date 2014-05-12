import orm
import os
import csv

path = os.path.dirname(os.path.realpath(__file__))
filename = os.path.join(path, 'indicator.csv')

field_names = ["indID", "name", "units"]

dataset = {"dsID": "fts",
           "last_updated": "",
           "last_scraped": orm.now(),
           "name": "Financial Tracking Service, OCHA"}


def indicators(filename):
    with open(filename, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            yield dict(zip(field_names, row))

orm.DataSet(**dataset).save()
for indicator in indicators(filename):
    print indicator
    orm.Indicator(**indicator).save()
