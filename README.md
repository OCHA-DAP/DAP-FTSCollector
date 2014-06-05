DAP-FTSCollector
================

Queries the [FTS API](http://fts.unocha.org/api/Files/APIUserdocumentation.htm) for funding data, which is then
aggregated into various humanitarian indicators.

The list of countries for which data is reported is fetched via this 
[Country JSON](http://fts.unocha.org/api/v1/Country.json)

The list of organizations for which data is reported is fetched via this
[Organization JSON](http://fts.unocha.org/api/v1/Organization.json)

Worldwide funding data is collected for each year:

  - grouped by country ([example JSON](http://fts.unocha.org/api/v1/funding.json?Year=2010&GroupBy=country))
    - FY630 is the total funding for each country that year
  - grouped by donor ([example JSON](http://fts.unocha.org/api/v1/funding.json?Year=2010&GroupBy=donor))
    - used to get pooled fund (CERF, ERF, CHF) totals (see below)

Then for each country:

- the list of all FTS appeals is fetched (see this [example JSON](http://fts.unocha.org/api/v1/Appeal/country/KEN.json))
  - the funding is grouped by year, for all appeals and CAP appeals
  - for each year:
    - FY010 is the sum of all appeals 'original_requirements'
    - FY020 is the sum of all appeals 'current_requirements'
    - FY040 is the sum of all appeals 'funding'
    - FA010 is the sum of CAP appeals 'current_requirements'
    - FA140 is the sum of CAP appeals 'funding'

- for each appeal, its funding details are fetched by recipient
  (see this [example JSON](http://fts.unocha.org/api/v1/funding.json?Appeal=984&GroupBy=Recipient))
  - from the earlier Organization JSON, we know the "type" of each recipient
  - the funding is rolled up across appeals by year and recipient type
     - FY190 is the sum of funding for NGOs
     - FY200 is the sum of funding for Private Organizations
     - FY210 is the sum of funding for UN Agencies
     - currently, other organization types are not reported

- the list of all FTS emergencies is fetched (see this [example JSON](http://fts.unocha.org/api/v1/Emergency/country/KEN.json))
  - for each emergency, the funding contributions are fetched ([example JSON](http://fts.unocha.org/api/v1/Contribution/emergency/15747.json)) 
  - contributions still in "pledge" status are excluded
  - contributions with the donor set as a pooled fund (CERF, ERF, or CHF) are kept
  - the contributions are rolled up across emergencies by year and donor
    - total pooled fund contributions:
      - FY240 is sum of all CERF contributions
      - FY380 is sum of all ERF contributions
      - FY520 is sum of all CHF contributions
    - the ratio of each pooled fund amount in that country vs the worldwide total (fetched earlier above):
      - FY360 is sum of all CERF contributions divided by total worldwide CERF contributions
      - FY500 is sum of all ERF contributions divided by total worldwide ERF contributions
      - FY540 is sum of all CHF contributions divided by total worldwide CHF contributions
    - the ratio of each pooled fund amount vs the total country funding (fetched earlier above):
      - FY370 is sum of all CERF contributions divided by total country funding 
      - FY510 is sum of all ERF contributions divided by total country funding
      - FY550 is sum of all CHF contributions divided by total country funding
    - FY620 is the sum of all pooled fund contributions for the country (the sum of FY240, FY380, and FY520)
