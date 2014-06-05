DAP-FTSCollector
================

Queries the [FTS API](http://fts.unocha.org/api/Files/APIUserdocumentation.htm) for funding data, which is then
aggregated into various humanitarian indicators.

The list of countries for which data is reported is available via this 
[Country JSON](http://fts.unocha.org/api/v1/Country.json)

The list of organizations for which data is reported is available via this
[Organization JSON](http://fts.unocha.org/api/v1/Organization.json)


For each country:

- the list of all FTS appeals is fetched (see this [example Appeals JSON](http://fts.unocha.org/api/v1/Appeal/country/KEN.json))
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
  - the funding is rolled up by year and recipient type
     - FY190 is the sum of funding for NGOs
     - FY200 is the sum of funding for Private Organizations
     - FY210 is the sum of funding for UN Agencies
     - currently, other organization types are not reported
