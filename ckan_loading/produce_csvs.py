"""
Can be used to produce the following CSV files for upload into CKAN:
  - sectors.csv
  - countries.csv
  - organizations.csv
  - emergencies.csv (for a given country)
  - appeals.csv (for a given country)
  - projects.csv (for a given country, based on appeals)
  - contributions.csv (for given country, based on emergencies, which should capture all appeals, also)
"""

import fts_queries
import os
import pandas as pd

# TODO extract strings to header section above the code


def build_csv_path(base_path, object_type, country=None):
    """
    Using CSV names that duplicate the file paths here, which generally I don't like,
    but having very explicit filenames is maybe nicer to sort out for CKAN.
    """
    filename = 'fts_' + object_type + '.csv'

    if country:  # a little bit of duplication but easier to read
        filename = 'fts_' + country + '_' + object_type + '.csv'

    return os.path.join(base_path, filename)


def write_dataframe_to_csv(dataframe, path):
    print "Writing", path
    # include the index which is an ID for each of the objects serialized by this script
    # use Unicode as many non-ASCII characters present in this data
    dataframe.to_csv(path, index=True, encoding='utf-8')


def filter_out_empty_dataframes(dataframes):
    # empty dataframes will fail the "if" test
    return [frame for frame in dataframes if not frame.empty]


def produce_sectors_csv(output_dir):
    sectors = fts_queries.fetch_sectors_json_as_dataframe()
    write_dataframe_to_csv(sectors, build_csv_path(output_dir, 'sectors'))


def produce_countries_csv(output_dir):
    countries = fts_queries.fetch_countries_json_as_dataframe()
    write_dataframe_to_csv(countries, build_csv_path(output_dir, 'countries'))


def produce_organizations_csv(output_dir):
    organizations = fts_queries.fetch_organizations_json_as_dataframe()
    write_dataframe_to_csv(organizations, build_csv_path(output_dir, 'organizations'))


def produce_global_csvs(base_output_dir):
    # not sure if this directory creation code should be somewhere else..?
    output_dir = os.path.join(base_output_dir, 'fts', 'global')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    produce_sectors_csv(output_dir)
    produce_countries_csv(output_dir)
    produce_organizations_csv(output_dir)


def produce_emergencies_csv_for_country(output_dir, country):
    emergencies = fts_queries.fetch_emergencies_json_for_country_as_dataframe(country)
    write_dataframe_to_csv(emergencies, build_csv_path(output_dir, 'emergencies', country=country))


def produce_appeals_csv_for_country(output_dir, country):
    appeals = fts_queries.fetch_appeals_json_for_country_as_dataframe(country)
    write_dataframe_to_csv(appeals, build_csv_path(output_dir, 'appeals', country=country))


def produce_projects_csv_for_country(output_dir, country):
    # first get all appeals for this country (could eliminate this duplicative call, but it's not expensive)
    appeals = fts_queries.fetch_appeals_json_for_country_as_dataframe(country)
    appeal_ids = appeals.index
    # then get all projects corresponding to those appeals and concatenate into one big frame
    list_of_projects = [fts_queries.fetch_projects_json_for_appeal_as_dataframe(appeal_id) for appeal_id in appeal_ids]
    list_of_non_empty_projects = filter_out_empty_dataframes(list_of_projects)
    projects_frame = pd.concat(list_of_non_empty_projects)
    write_dataframe_to_csv(projects_frame, build_csv_path(output_dir, 'projects', country=country))


def produce_contributions_csv_for_country(output_dir, country):
    # first get all emergencies for this country (could eliminate this duplicative call, but it's not expensive)
    emergencies = fts_queries.fetch_emergencies_json_for_country_as_dataframe(country)
    emergency_ids = emergencies.index
    # then get all contributions corresponding to those emergencies and concatenate into one big frame
    list_of_contributions = [fts_queries.fetch_contributions_json_for_emergency_as_dataframe(emergency_id)
                             for emergency_id in emergency_ids]
    list_of_non_empty_contributions = filter_out_empty_dataframes(list_of_contributions)
    contributions_master_frame = pd.concat(list_of_non_empty_contributions)
    write_dataframe_to_csv(contributions_master_frame, build_csv_path(output_dir, 'contributions', country=country))


def produce_csvs_for_country(base_output_dir, country):
    output_dir = os.path.join(base_output_dir, 'fts', 'per_country', country)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    produce_emergencies_csv_for_country(output_dir, country)
    produce_appeals_csv_for_country(output_dir, country)
    produce_projects_csv_for_country(output_dir, country)
    produce_contributions_csv_for_country(output_dir, country)


if __name__ == "__main__":
    # output all CSVs for the given countries to '/tmp/'
    country_codes = ['COL', 'SSD', 'YEM', 'PAK']  # the set of starter countries for DAP
    tmp_output_dir = '/tmp/'

    produce_global_csvs(tmp_output_dir)
    for country_code in country_codes:
        produce_csvs_for_country(tmp_output_dir, country_code)
