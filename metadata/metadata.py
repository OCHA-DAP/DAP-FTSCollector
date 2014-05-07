import orm

chd_codes = """
FY010|Humanitarian appeals: Amount requested (original)|USD
FY020|Humanitarian appeals: Amount requested (revised)|USD
FY040|Humanitarian appeals: Amount received by cluster (total)|USD
FY030|Humanitarian appeals: Percentage covered|percent
FA010|CAP: Revised amount required by cluster (total)|USD
FA140|CAP: Amount recieved by cluster (total)|USD
FY190|Humanitarian appeals: Amount recieved by NGOs|USD
FY200|Humanitarian appeals: Amount recieved by private organisations and foundations|USD
FY210|Humanitarian appeals: Amount recieved by UN Agencies|USD
FY240|CERF: Allocations by cluster (total)|USD
FY360|CERF: Amount allocated as percentage of total CERF global fund allocations|percent
FY370|CERF: Amount allocated as percentage of total country humanitarian funding receieved|percent
FY520|Common Humanitarian Fund: Contributions by donor (total)|USD
FY540|Common Humanitarian Fund: Allocations by sector (total)|USD
FY550|Common Humanitarian Fund: Amount allocated as percentage of total country humanitarian funding received|percent
FY380|Emergency Response Fund: Contributions by donor (total)|USD
FY500|Emergency Response Fund: Amount allocated as percentage of total ERF global funding|percent
FY510|Emergency Response Fund: Amount allocated as percentage of total country humanitarian funding received|percent
FY620|Total country pooled fund allocations|USD
FY630|Total country humanitarian funding received|USD
FY640|Country pooled funding received as percentage of total funding received|percent
"""

dataset = {"dsID": "fts",
           "last_updated": "",
           "last_scraped": orm.now(),
           "name": "Financial Tracking Service, OCHA"}

orm.DataSet(**dataset).save()

def indicators():
    field_names = ["indID", "name", "units"]
    for row in chd_codes.strip().split('\n'):
        yield dict(zip(field_names, row.split("|")))

for indicator in indicators():
    orm.Indicator(**indicator).save()
