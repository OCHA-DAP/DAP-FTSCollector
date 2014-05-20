"""
Provides queries against the FTS API, fetching JSON and translating it into pandas dataframes.
It unfortunately doesn't show the structure of the returned data explicitly, that's all handled by pandas.
At some point we may want to create dedicated classes for each type of data returned by the API, to do validation etc,
but then we'll also need to implement join logic between these classes.

For more information on the FTS API see http://fts.unocha.org/api/Files/APIUserdocumentation.htm
"""

import pandas as pd

FTS_BASE_URL = 'http://fts.unocha.org/api/v1/'
JSON_SUFFIX = '.json'


# leading underscores indicate "private"/internal functions


def _fetch_json_as_dataframe(url):
    """
    pandas will fetch the given JSON URL and try to build a dataframe from the contents
    """
    return pd.read_json(url)


def _fetch_json_as_dataframe_with_id(url):
    """
    Fetch a JSON url as a dataframe, using the "id" field as the "index" ("key") of the dataframe
    """
    dataframe = _fetch_json_as_dataframe(url)
    if 'id' in dataframe.columns:
        return dataframe.set_index('id')
    else:
        return dataframe  # happens with an empty result


def _build_json_url(middle_part):
    """
    FTS URLs have a fairly standard pattern - a base, a descriptive "middle part", and a generic JSON suffix
    """
    return FTS_BASE_URL + middle_part + JSON_SUFFIX


def _convert_date_columns_from_string_to_timestamp(dataframe, column_names):
    for column_name in column_names:
        dataframe[column_name] = dataframe[column_name].apply(pd.datetools.parse)


def fetch_sectors_json_as_dataframe():
    """
    Returns the canonical "Sectors" available in FTS data
    Columns:
    id - sector id within FTS
    name - sector name within FTS
    """
    return _fetch_json_as_dataframe_with_id(_build_json_url('Sector'))


def fetch_countries_json_as_dataframe():
    """
    Returns the countries known to FTS
    Columns:
    id - country id within FTS
    iso_code_A - alphabetic ISO code for each country (e.g. 'KEN' for Kenya)
    iso_code_N - numeric ISO code for each country (e.g. 32 for Argentina)
    name - FTS name for each country
    """
    return _fetch_json_as_dataframe_with_id(_build_json_url('Country'))


def fetch_organizations_json_as_dataframe():
    """
    Returns organizations known to FTS
    Columns:
    id - organization id within FTS
    abbreviation - short name for the organization, e.g. 'HOPE' for Humanitarian Organization for Poverty Elimination
    name - name of the organization
    type - type of organization, e.g. 'NGOs', 'UN Agencies', etc
    """
    return _fetch_json_as_dataframe_with_id(_build_json_url('Organization'))


def fetch_emergencies_json_for_country_as_dataframe(country):
    """
    Returns all known FTS Emergencies for the given country.
    Accepts both names ("Slovakia") and ISO country codes ("SVK").
    Columns:
    id - the FTS emergency id
    country - the full name of the country, e.g. "Slovakia"
    funding - how much USD funding (excluding pledges) each emergency has received
    glideid - not sure what this is
    pledges - how much USD funding is currently in pledge status for each emergency
    title - the name of the emergency
    type - the type of emergency, e.g. 'Natural Disaster'
    year - the year of the emergency, e.g. 2012
    """
    return _fetch_json_as_dataframe_with_id(_build_json_url('Emergency/country/' + country))


def fetch_emergencies_json_for_year_as_dataframe(year):
    """
    Similar to fetch_emergencies_json_for_country_as_dataframe,
    except it finds all FTS emergencies for a given year (e.g. 2012).
    """
    return _fetch_json_as_dataframe_with_id(_build_json_url('Emergency/year/' + str(year)))


def _fetch_appeals_json_as_dataframe_given_url(url):
    """
    Fetches appeals JSON and converts columns to better datatypes
    """
    dataframe = _fetch_json_as_dataframe_with_id(url)
    if not dataframe.empty:
        _convert_date_columns_from_string_to_timestamp(dataframe, ['start_date', 'end_date', 'launch_date'])
    return dataframe


def fetch_appeals_json_for_country_as_dataframe(country):
    """
    Returns all known FTS Appeals for the given country.
    Accepts both names ("Slovakia") and ISO country codes ("SVK").
    Columns:
    id - the FTS appeal id
    emergency_id - the id for the FTS emergency associated with this appeal
    country - the full name of the country, e.g. "Slovakia"
    current_requirements - a revised amount of USD funding required by the appeal
    original_requirements - the original amount of USD funding required by the appeal
    funding - how much USD funding (excluding pledges) each appeal has received
    pledges - how much USD funding is currently in pledge status for each emergency
    title - the name of the appeal
    type - the type of appeal, e.g. 'CAP', 'FLASH', etc
    year - the year of the appeal, e.g. 2012
    start_date - ? the start of the appeal period?
    end_date - ? the end of the appeal period?
    launch_date - the date the appeal was launched
    """
    return _fetch_appeals_json_as_dataframe_given_url(_build_json_url('Appeal/country/' + country))


def fetch_appeals_json_for_year_as_dataframe(year):
    """
    Similar to fetch_appeals_json_for_country_as_dataframe,
    except it finds all FTS appeals for a given year (e.g. 2012).
    """
    return _fetch_appeals_json_as_dataframe_given_url(_build_json_url('Appeal/year/' + str(year)))


def fetch_projects_json_for_appeal_as_dataframe(appeal_id):
    """
    Returns all known projects for a given appeal.
    Columns:
    id - the FTS id for each project
    title - the title of the project, e.g. 'Control of Locust Invasion in Turkana Kenya'
    objective - a text description of what the project hopes to achieve
    priority - a string like 'HIGH' or 'MEDIUM'
    appeal_id - the id of the associated FTS appeal
    appeal_title - the title of the associated FTS appeal
    cluster - the associated cluster of the project. Note that this does not seem to be standardized across appeals.
    sector - the canonical FTS sector for this project
    code - a project code, not sure who assigns it
    country - the recipient country associated with the appeal
    organisation - the name of the organization carrying out the project, e.g. "Kenyan Red Cross Society"
    organisation_abbreviation - an abbreviated name of the organization, e.g. "UNDP"
    current_requirements - the revised funding requirements of the project
    original_requirements - the original funding requirements of the project
    gendermarker - the IASC gender marker code
    end_date - ? not sure what this really means
    last_updated_datetime - ? not sure what this really means
    """
    dataframe = _fetch_json_as_dataframe_with_id(_build_json_url('Project/appeal/' + str(appeal_id)))
    if not dataframe.empty:  # guard against empty result
        _convert_date_columns_from_string_to_timestamp(dataframe, ['end_date', 'last_updated_datetime'])
    return dataframe


def fetch_clusters_json_for_appeal_as_dataframe(appeal_id):
    # NOTE no id present in this data
    return _fetch_json_as_dataframe(_build_json_url('Cluster/appeal/' + str(appeal_id)))


def _fetch_contributions_json_as_dataframe_given_url(url):
    """
    Fetches contributions JSON and converts columns to better datatypes
    """
    dataframe = _fetch_json_as_dataframe_with_id(url)
    if not dataframe.empty:  # guard against empty result
        _convert_date_columns_from_string_to_timestamp(dataframe, ['decision_date'])
    return dataframe


def fetch_contributions_json_for_appeal_as_dataframe(appeal_id):
    """
    Returns all known funding contributions towards an appeal.
    Columns:
    id - the FTS id for the contribution
    amount - the USD amount of the contribution
    appeal_id - the id of the associated FTS appeal (can be 0 if not associated with an appeal)
    appeal_title - the title of the associated FTS appeal
    emergency_id - the id of the associated FTS emergency
    emergency_title - the title of the associated FTS emergency
    donor - a string describing the donor nation or organization
    recipient - the name of the organization receiving the contribution
    project_code - the code for the project the contribution is funding
    status - one of 'Pledge', 'Commitment', 'Paid contribution'.
       Pledges seem to be considered not final enough to be included in "funding".
    is_allocation - 0 or 1 depending on if this is an allocation of some larger funding initiative, e.g. pooled funds?
    year - the year of the contribution? or the year of the appeal/emergency?
    decision_date - not sure what this is
    """
    return _fetch_contributions_json_as_dataframe_given_url(_build_json_url('Contribution/appeal/' + str(appeal_id)))


def fetch_contributions_json_for_emergency_as_dataframe(emergency_id):
    """
    Similar to fetch_contributions_json_for_appeal_as_dataframe,
    except it finds all contributions for a given emergency id.
    """
    return _fetch_contributions_json_as_dataframe_given_url(
        _build_json_url('Contribution/emergency/' + str(emergency_id)))


def _fetch_grouping_type_json_as_dataframe(middle_part, query, grouping, alias):
    """
    Query can be one of:
        Emergency=X
        Appeal=X
        Country=X
        Donor=X
        Recipient=X
        Year=X
    Grouping can be one of:
        Donor
        Recipient
        Sector
        Emergency
        Appeal
        Country
        Cluster
    Alias is used to name the grouping type column and use it as an index.
    """
    url = _build_json_url(middle_part) + '?' + query

    if grouping:
        url += '&GroupBy=' + grouping

    # NOTE no id present in this data
    raw_dataframe = _fetch_json_as_dataframe(url)

    # oddly the JSON of interest is nested inside the "grouping" element
    processed_frame = pd.DataFrame.from_records(raw_dataframe.grouping.values)

    if processed_frame.empty:
        return processed_frame

    if alias:
        processed_frame = processed_frame.rename(columns={'type': alias, 'amount': middle_part})
        processed_frame = processed_frame.set_index(alias)

    return processed_frame


def fetch_grouping_type_json_for_appeal_as_dataframe(middle_part, appeal_id, grouping, alias):
    return _fetch_grouping_type_json_as_dataframe(middle_part, 'Appeal=' + str(appeal_id), grouping, alias)


def fetch_grouping_type_json_for_emergency_as_dataframe(middle_part, emergency_id, grouping, alias):
    return _fetch_grouping_type_json_as_dataframe(middle_part, 'Emergency=' + str(emergency_id), grouping, alias)


def fetch_grouping_type_json_for_year_as_dataframe(middle_part, year, grouping, alias):
    return _fetch_grouping_type_json_as_dataframe(middle_part, 'Year=' + str(year), grouping, alias)


def fetch_funding_json_for_appeal_as_dataframe(appeal_id, grouping, alias):
    """
    Committed or contributed funds, including carry over from previous years
    """
    return fetch_grouping_type_json_for_appeal_as_dataframe("funding", appeal_id, grouping, alias)


def fetch_funding_json_for_emergency_as_dataframe(emergency_id, grouping, alias):
    """
    Committed or contributed funds, including carry over from previous years
    """
    return fetch_grouping_type_json_for_emergency_as_dataframe("funding", emergency_id, grouping, alias)


def fetch_pledges_json_for_appeal_as_dataframe(appeal_id, grouping, alias):
    """
    Contains uncommitted pledges, not funding that has already processed to commitment or contribution stages
    """
    return fetch_grouping_type_json_for_appeal_as_dataframe("pledges", appeal_id, grouping, alias)


if __name__ == "__main__":
    # test various fetch commands (requires internet connection)
    country = 'Chad'
    appeal_id = 942

    print fetch_sectors_json_as_dataframe()
    print fetch_emergencies_json_for_country_as_dataframe(country)
    print fetch_projects_json_for_appeal_as_dataframe(appeal_id)
    print fetch_funding_json_for_appeal_as_dataframe(appeal_id)
