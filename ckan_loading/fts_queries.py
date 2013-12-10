"""
Provides queries against the FTS API, fetching JSON and translating it into pandas dataframes.
It unfortunately doesn't show the structure of the returned data explicitly, that's all handled by pandas.
At some point we may want to create dedicated classes for each type of data returned by the API, to do validation etc,
but then we'll also need to implement join logic between these classes.
"""

import pandas as pd

FTS_BASE_URL = 'http://fts.unocha.org/api/v1/'
JSON_SUFFIX = '.json'


def fetch_json_as_dataframe(url):
    return pd.read_json(url)


def fetch_json_as_dataframe_with_id(url):
    dataframe = fetch_json_as_dataframe(url)
    if 'id' in dataframe.columns:
        return dataframe.set_index('id')
    else:
        return dataframe  # happens with an empty result


def build_json_url(middle_part):
    return FTS_BASE_URL + middle_part + JSON_SUFFIX


def convert_date_columns_from_string_to_timestamp(dataframe, column_names):
    for column_name in column_names:
        dataframe[column_name] = dataframe[column_name].apply(pd.datetools.parse)


def fetch_sectors_json_as_dataframe():
    return fetch_json_as_dataframe_with_id(build_json_url('Sector'))


def fetch_countries_json_as_dataframe():
    return fetch_json_as_dataframe_with_id(build_json_url('Country'))


def fetch_organizations_json_as_dataframe():
    return fetch_json_as_dataframe_with_id(build_json_url('Organization'))


def fetch_emergencies_json_for_country_as_dataframe(country):
    """
    This accepts both names ("Slovakia") and ISO country codes ("SVK")
    """
    return fetch_json_as_dataframe_with_id(build_json_url('Emergency/country/' + country))


def fetch_emergencies_json_for_year_as_dataframe(year):
    return fetch_json_as_dataframe_with_id(build_json_url('Emergency/year/' + str(year)))


def fetch_appeals_json_as_dataframe_given_url(url):
    dataframe = fetch_json_as_dataframe_with_id(url)
    convert_date_columns_from_string_to_timestamp(dataframe, ['start_date', 'end_date', 'launch_date'])
    return dataframe


def fetch_appeals_json_for_country_as_dataframe(country):
    """
    This accepts both names ("Slovakia") and ISO country codes ("SVK")
    """
    return fetch_appeals_json_as_dataframe_given_url(build_json_url('Appeal/country/' + country))


def fetch_appeals_json_for_year_as_dataframe(year):
    return fetch_appeals_json_as_dataframe_given_url(build_json_url('Appeal/year/' + str(year)))


def fetch_projects_json_for_appeal_as_dataframe(appeal_id):
    dataframe = fetch_json_as_dataframe_with_id(build_json_url('Project/appeal/' + str(appeal_id)))
    if not dataframe.empty:  # guard against empty result
        convert_date_columns_from_string_to_timestamp(dataframe, ['end_date', 'last_updated_datetime'])
    return dataframe


def fetch_clusters_json_for_appeal_as_dataframe(appeal_id):
    # NOTE no id present in this data
    return fetch_json_as_dataframe(build_json_url('Cluster/appeal/' + str(appeal_id)))


def fetch_contributions_json_as_dataframe_given_url(url):
    dataframe = fetch_json_as_dataframe_with_id(url)
    if not dataframe.empty:  # guard against empty result
        convert_date_columns_from_string_to_timestamp(dataframe, ['decision_date'])
    return dataframe


def fetch_contributions_json_for_appeal_as_dataframe(appeal_id):
    return fetch_contributions_json_as_dataframe_given_url(build_json_url('Contribution/appeal/' + str(appeal_id)))


def fetch_contributions_json_for_emergency_as_dataframe(emergency_id):
    return fetch_contributions_json_as_dataframe_given_url(
        build_json_url('Contribution/emergency/' + str(emergency_id)))


def fetch_grouping_type_json_for_appeal_as_dataframe(middle_part, appeal_id, grouping=None, alias=None):
    """
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
    url = build_json_url(middle_part) + '?Appeal=' + str(appeal_id)

    if grouping:
        url += '&GroupBy=' + grouping

    # NOTE no id present in this data
    raw_dataframe = fetch_json_as_dataframe(url)

    # oddly the JSON of interest is nested inside the "grouping" element
    processed_frame = pd.DataFrame.from_records(raw_dataframe.grouping.values)

    if alias:
        processed_frame = processed_frame.rename(columns={'type': alias, 'amount': middle_part})
        processed_frame = processed_frame.set_index(alias)

    return processed_frame


def fetch_funding_json_for_appeal_as_dataframe(appeal_id, grouping=None, alias=None):
    """
    Committed or contributed funds, including carry over from previous years
    """
    return fetch_grouping_type_json_for_appeal_as_dataframe("funding", appeal_id, grouping, alias)


def fetch_pledges_json_for_appeal_as_dataframe(appeal_id, grouping=None, alias=None):
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
