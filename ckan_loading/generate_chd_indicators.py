"""
Builds CHD indicators from FTS queries
"""

import fts_queries
import os
import datetime
import sqlite3
import pandas as pd
import pandas.io.sql as sql


# note relying on strings is fragile - could break if things get renamed in FTS
# we don't seem to have much in the way of alternatives, other than changing the FTS API
DONOR_ERF = "Emergency Response Fund (OCHA)"
DONOR_CERF = "Central Emergency Response Fund"
DONOR_CHF = "Common Humanitarian Fund"
POOLED_FUNDS = [DONOR_CERF, DONOR_ERF, DONOR_CHF]


class PooledFundCacheByYear(object):
    """
    Caches global pooled fund amounts by year
    """
    def __init__(self):
        self.year_cache = {}

    # TODO investigate why this doesn't match FTS reports exactly for all values
    def get_pooled_global_allocation_for_year(self, year):
        if year not in self.year_cache:
            global_funding_by_donor =\
                fts_queries.fetch_grouping_type_json_for_year_as_dataframe('funding', year, 'donor', 'organization')

            pooled_funds_amounts = global_funding_by_donor.funding.loc[POOLED_FUNDS]

            self.year_cache[year] = pooled_funds_amounts

        return self.year_cache[year]


class CountryFundingCacheByYear(object):
    """
    Caches total funding amounts for each country by year
    """
    def __init__(self):
        self.year_cache = {}

        self.country_iso_code_to_name = {}
        countries = fts_queries.fetch_countries_json_as_dataframe()
        for country_id, row in countries.iterrows():
            self.country_iso_code_to_name[row['iso_code_A']] = row['name']

    def get_total_country_funding_for_year(self, country_code, year):
        if year not in self.year_cache:
            funding_by_country =\
                fts_queries.fetch_grouping_type_json_for_year_as_dataframe('funding', year, 'country', 'country')

            self.year_cache[year] = funding_by_country

        # possibly no funding at all in that year
        funding_series = self.year_cache[year]
        if funding_series.empty:
            return 0

        country_name = self.country_iso_code_to_name[country_code]

        if country_name in funding_series.funding:
            return funding_series.funding.loc[country_name]
        else:
            return 0


POOLED_FUND_CACHE = PooledFundCacheByYear()
COUNTRY_FUNDING_CACHE = CountryFundingCacheByYear()

FUNDING_STATUS_PLEDGE = "Pledge"

ORG_TYPE_NGOS = 'NGOs'
ORG_TYPE_PRIVATE_ORGS = 'Private Orgs. & Foundations'
ORG_TYPE_UN_AGENCIES = 'UN Agencies'


# holds IndicatorValue objects until we are ready to put them in a dataframe
VALUES = []


class IndicatorValue(object):
    def __init__(self, indicator, region, year, value):
        self.indicator = indicator
        self.region = region
        self.year = year
        self.value = value


def add_row_to_values(indicator, region, year, value):
    # perhaps the wrong place to add this, but filter out distant future data
    if year > datetime.date.today().year + 5:
        return

    VALUES.append(IndicatorValue(indicator, region, year, value))


def get_values_as_dataframe():
    indicators = [ind_value.indicator for ind_value in VALUES]
    regions = [ind_value.region for ind_value in VALUES]
    years = [ind_value.year for ind_value in VALUES]
    values = [ind_value.value for ind_value in VALUES]

    return pd.DataFrame(
        {'indicator': indicators, 'region': regions, 'year': years, 'value': values},
        columns=['indicator', 'region', 'year', 'value']
    )


def write_values_as_scraperwiki_style_csv(base_dir):
    values = get_values_as_dataframe()
    values.replace(to_replace=[float('inf')],
                   value=['na'],
                   inplace=True)
    values['dsID'] = 'fts'
    values['is_number'] = 1
    values['source'] = ''
    values = values.rename(columns={'indicator': 'indID', 'year': 'period'})
    values = values[['dsID', 'region', 'indID', 'period', 'value', 'is_number', 'source']]

    filename = os.path.join(base_dir, 'value.csv')
    values.to_csv(filename, index=False)


def write_values_as_scraperwiki_style_sql(base_dir):
    TABLE_NAME = "value"
    values = get_values_as_dataframe()
    values.replace(to_replace=[float('inf')],
                   value=['na'],
                   inplace=True)
    values['dsID'] = 'fts'
    values['is_number'] = 1
    values['source'] = ''
    values = values.rename(columns={'indicator': 'indID', 'year': 'period'})
    values = values[['dsID', 'region', 'indID', 'period', 'value', 'is_number', 'source']]

    filename = os.path.join(base_dir, 'ocha.db')
    sqlite_db = sqlite3.connect(filename)
    sqlite_db.execute("drop table if exists {};".format(TABLE_NAME))
    values = values.reset_index()
    sql.write_frame(values, TABLE_NAME, sqlite_db)
    print values

def get_values_joined_with_indicators():
    """
    Useful for debugging
    """
    values = get_values_as_dataframe()
    indicators = pd.read_csv('indicator.csv', index_col='indID')
    return pd.merge(left=values, right=indicators, left_on='indicator', right_index=True)


def populate_appeals_level_data(country):
    appeals = fts_queries.fetch_appeals_json_for_country_as_dataframe(country)

    if appeals.empty:
        return

    # group all appeals by year, columns are now just the numerical ones:
    #  - current_requirements, emergency_id, funding, original_requirements, pledges
    cross_appeals_by_year = appeals.groupby('year').sum().astype(float)

    # iterating by rows is not considered proper form to but it's easier to follow
    for year, row in cross_appeals_by_year.iterrows():
        add_row_to_values('FY010', country, year, row['original_requirements'])
        add_row_to_values('FY020', country, year, row['current_requirements'])
        add_row_to_values('FY040', country, year, row['funding'])

        # note this is a fraction, not a percent, and not clipped to 100%
        add_row_to_values('FY030', country, year, row['funding'] / row['current_requirements'])

    # CAP-only
    cap_appeals = appeals[appeals.type == 'CAP']
    for appeal_id, row in cap_appeals.iterrows():
        add_row_to_values('FA010', country, row['year'], row['current_requirements'])
        add_row_to_values('FA140', country, row['year'], row['funding'])


def get_organizations_indexed_by_name():
    """
    Load organizations from FTS and change index to be name, as sadly that's what is used in
    other API calls, so we need to join on it.
    This is a slow call, so it makes sense to cache it.
    """
    organizations = fts_queries.fetch_organizations_json_as_dataframe()
    return organizations.set_index('name')


def populate_organization_level_data(country, organizations=None):
    if organizations is None:
        organizations = get_organizations_indexed_by_name()

    # load appeals, analyze each one
    appeals = fts_queries.fetch_appeals_json_for_country_as_dataframe(country)

    funding_dataframes_by_appeal = []

    for appeal_id, appeal_row in appeals.iterrows():
        # first check if there is any funding at all (otherwise API calls will get upset)
        if appeal_row['funding'] == 0:
            continue

        # query funding by recipient, including "carry over" from previous years
        funding_by_recipient = fts_queries.fetch_funding_json_for_appeal_as_dataframe(
            appeal_id, grouping='Recipient', alias='organisation')

        funding_by_recipient['year'] = appeal_row['year']

        funding_dataframes_by_appeal.append(funding_by_recipient)

    if not funding_dataframes_by_appeal:
        return

    funding_by_recipient_overall = pd.concat(funding_dataframes_by_appeal)

    # now roll up by organization type
    funding_by_type = funding_by_recipient_overall.join(organizations.type).groupby(['type', 'year']).sum()

    for (org_type, year), row in funding_by_type.iterrows():
        if org_type == ORG_TYPE_NGOS:
            add_row_to_values('FY190', country, year, row['funding'])
        elif org_type == ORG_TYPE_PRIVATE_ORGS:
            add_row_to_values('FY200', country, year, row['funding'])
        elif org_type == ORG_TYPE_UN_AGENCIES:
            add_row_to_values('FY210', country, year, row['funding'])
        else:
            # note that some organizations (e.g. Red Cross/Red Crescent) fall outside the 3 indicator categories
            print 'Ignoring funding for organization type ' + org_type


def populate_pooled_fund_data(country):
    emergencies = fts_queries.fetch_emergencies_json_for_country_as_dataframe(country)

    contribution_dataframes_by_emergency = []

    for emergency_id, emergency_row in emergencies.iterrows():
        contributions = fts_queries.fetch_contributions_json_for_emergency_as_dataframe(emergency_id)

        if contributions.empty:
            continue

        # note that is_allocation field is much cleaner and _almost_ gives the same answer,
        # but found 1 instance of contribution that did not have this field set and yet looked like it should

        # exclude pledges
        contributions = contributions[contributions.status != FUNDING_STATUS_PLEDGE]

        # exclude non-CERF/ERF/CHF
        donor_filter = lambda x: x in POOLED_FUNDS
        contributions = contributions[contributions.donor.apply(donor_filter)]

        if contributions.empty:
            continue  # if not excluded, can mess up concat

        contribution_dataframes_by_emergency.append(contributions)

    if not contribution_dataframes_by_emergency:
        return

    contributions_overall = pd.concat(contribution_dataframes_by_emergency)

    # sum amount by donor-year
    amount_by_donor_year = contributions_overall.groupby(['donor', 'year']).amount.sum()

    for (donor, year), amount in amount_by_donor_year.iteritems():
        global_allocations = POOLED_FUND_CACHE.get_pooled_global_allocation_for_year(year)
        country_funding = COUNTRY_FUNDING_CACHE.get_total_country_funding_for_year(country, year)

        # note that 'global_allocations' is close to FTS report numbers but not always exactly the same
        # so FY360, FY500, FY540 are perhaps slightly off

        if donor == DONOR_CERF:
            add_row_to_values('FY240', country, year, amount)
            add_row_to_values('FY360', country, year, amount/global_allocations[DONOR_CERF])
            add_row_to_values('FY370', country, year, amount/country_funding)
        elif donor == DONOR_ERF:
            add_row_to_values('FY380', country, year, amount)
            add_row_to_values('FY500', country, year, amount/global_allocations[DONOR_ERF])
            add_row_to_values('FY510', country, year, amount/country_funding)
        elif donor == DONOR_CHF:
            add_row_to_values('FY520', country, year, amount)
            add_row_to_values('FY540', country, year, amount/global_allocations[DONOR_CHF])
            add_row_to_values('FY550', country, year, amount/country_funding)
        else:
            raise Exception('Unexpected donor:' + donor)

    # collapse funding across the pooled funds for each year, compare to total country funding
    for year, pooled_funding in amount_by_donor_year.sum(level=1).iteritems():
        country_funding = COUNTRY_FUNDING_CACHE.get_total_country_funding_for_year(country, year)

        add_row_to_values('FY620', country, year, pooled_funding)
        add_row_to_values('FY630', country, year, country_funding)
        add_row_to_values('FY640', country, year, pooled_funding/country_funding)


def populate_data_for_regions(region_list):
    # cache organizations as it's an expensive call
    organizations = get_organizations_indexed_by_name()

    for region in region_list:
        print "Populating indicators for region", region
        populate_appeals_level_data(region)
        populate_organization_level_data(region, organizations)
        populate_pooled_fund_data(region)


if __name__ == "__main__":
    # regions_of_interest = ['COL', 'KEN', 'YEM']
    # regions_of_interest = ['SSD']  # useful for testing CHF
    regions_of_interest = fts_queries.fetch_countries_json_as_dataframe().iso_code_A

    populate_data_for_regions(regions_of_interest)

    # print get_values_as_dataframe()
    # print get_values_joined_with_indicators()
    write_values_as_scraperwiki_style_csv('/tmp')
    write_values_as_scraperwiki_style_sql('/home/')
