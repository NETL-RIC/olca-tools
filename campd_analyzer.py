#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# campd_analyzer.py
#
##############################################################################
# REQUIRED IMPORTS
##############################################################################
import datetime
import glob
import os
import re
import time

import numpy as np
import pandas as pd


##############################################################################
# DOCUMENTATION
##############################################################################
__doc__ = """
This module was designed to analyze the EPA CAMPD CSV archives created by
the :func:`archive_epa_cams` function found in the utils.py module of the
ElectricityLCI Python package (https://github.com/NETL-RIC/ElectricityLCI).

The :func:`run` method checks a directory for CSV files, reads the CSV file
contents, counts the number of lines, and does a cursory check to see that
each month has data (``archive_epa_cams`` queries the EPA API for each
month in a given year; therefore, a failed request may result in missing
data for a single month).

The :func:`query_epa_cams` method is a derivative of ElectricityLCI's
:func:`archive_epa_cams` method, but allows the user to query a single
month for a single state in an effort to gap fill months.

There may be an instance of running :func:`archive_epa_cams` multiple times
in an attempt to create a complete time series (e.g., months Jan. and Feb.
were unsuccessful in the first API call and months Sep. and Dec. failed
to return data in a second API call).

It may be possible to merge these two datasets together.
The function, :func:`find_duplicate_archives` was created to identify pairs of
CSV files (an original and a duplicate) based on a user's naming scheme
(e.g., by adding 'ABCD' to one of the CSV's file name).

The :func:`fix` method finds all duplicated pairs, reads both, merges their
content, drops duplicates, sorts by date, and writes back to CSV in an attempt
to create complete CSV files that will not trip the :func:`run` method in
subsequent runs.

Author:
    Tyler W. Davis

Last updated:
    2025-10-24
"""


##############################################################################
# FUNCTIONS
##############################################################################
def analyze_df(df):
    """Analyze a data frame for number of lines (excluding the header) and
    for data representing a complete time series (i.e., at least one data
    point in each month).

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame with a 'Date' column.

    Returns
    -------
    tuple
        A tuple of length three:

        - (int, None) The number of data lines in the data frame or none.
        - (int) The number of missing months
        - (list) The integer representation of months missing (e.g., [1, 2])
    """
    # Add a check in case you send the file path instead of the data frame.
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Method expects a DataFrame, not %s!" % type(df))

    all_months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    num_lines = len(df)
    date_col = find_column(df, 'date')
    if date_col is None:
        # Use NoneType for line numbers to let :func:`run` know the date col
        # was not found; distinguish it from a file with zero lines.
        return (None, 12, all_months)

    # Unique list of months in the data frame (e.g., 1, 2, 10, 12)
    months = np.unique(
        [extract_year_month(x)[1] for x in df[date_col].values]
    )

    # Run two checks on list of months
    num_months = len(months)
    sum_months = months.sum()
    if num_months == 12 and sum_months == 78:
        return (num_lines, 0, [])
    elif num_months != 12:
        missed_mos = [x for x in all_months if x not in months]
        num_missed = len(missed_mos)
        return (num_lines, num_missed, missed_mos)


def build_glob(data_dir, year=None, freq=None):
    """Helper method to create a glob string.

    Parameters
    ----------
    data_dir : str
        A directory path where the EPA CAMPD archive CSV files are located.
    year : int, optional
        The year to search for, by default None
    freq : str, optional
        A choice between 'hourly', 'daily' and None (i.e., both), by default None

    Returns
    -------
    str
        A glob string based on the criteria provided.

    Examples
    --------
    >>> build_glob("data", 2016, 'hourly') # all 2016 hourly CSV files
    'data/epacems_hourly_2016*csv'
    >>> build_glob("data", None, 'daily') # all daily CSV files
    'data/epacems_daily_*csv'
    >>> build_glob("data") # all CSV files
    'data/epacems*.csv'
    """
    # Universal glob:
    my_glob = os.path.join(data_dir, "epacems*.csv")
    # Parameter-based globs:
    if year is not None and freq is not None:
        my_glob = os.path.join(data_dir, "epacems_%s_%d*csv" % (freq, year))
    elif year is not None and freq is None:
        my_glob = os.path.join(data_dir, "epacems_*_%d*csv" % year)
    elif year is None and freq is not None:
        my_glob = os.path.join(data_dir, "epacems_%s_*csv" % freq)

    return my_glob


def extract_year_month(d_str):
    """Helper method to extract year (int) and month (int) from a string."""
    try:
        d_obj = datetime.datetime.strptime(d_str,  "%Y-%m-%dT%H:%M:%S+00")
    except (ValueError, TypeError):
        # Provide additional utility with a secondary check
        try:
            d_obj = datetime.datetime.strptime(d_str,  "%Y-%m-%d")
        except (ValueError, TypeError):
            return (None, None)
        else:
            return (d_obj.year, d_obj.month)
    else:
        return (d_obj.year, d_obj.month)


def find_column(df, col_name):
    """Helper method to find a given column.
    Returns string name or None if not found."""
    if not isinstance(df, pd.DataFrame) or not isinstance(col_name, str):
        raise TypeError(
            "Method requires a DataFrame and str, not %s and %s" % (
                type(df), type(col_name)
            )
        )

    my_col = [x for x in df.columns if x.lower() == col_name.lower()]
    if len(my_col) == 1:
        my_col = my_col[0]
    else:
        my_col = None

    return my_col


def find_duplicate_archives(data_dir, duplicate_str):
    """Search a data directory for files marked with duplicate string, and
    return a list of tuples of original CSV files and their duplicates.

    This method is to assist with joining multiple CSV archives of the same
    EPA CAMPD year-state. For example, if the archive EPA CAMPD method in
    ElectricityLCI ran once and, based on the :func:`run` method in this
    module, a year-state CSV file was found to be deficient in X number of
    months, such that the archive method was run a second time to try to
    capture the missing data. The first-run CSV was given some dummy text
    to its file name (e.g. 'ABCD') such that the archive method failed to find
    the CSV and queried the API again. This creates two CSV files: the one
    from the first pass (dup_file), and the one from the second pass
    (orig_file).

    Parameters
    ----------
    data_dir : str
        The data directory path.
    duplicate_str : str
        The search string that distinguished a duplicated CSV file from its
        original.

    Returns
    -------
    list
        A list of tuples. Each tuple is length two: the file path without the
        duplicate string and the file path with the duplicate string.
    """
    # Get all files
    all_files = glob.glob(build_glob(data_dir))

    # Create the regular expression for searching file names
    p = re.compile(".*%s.*" % duplicate_str, re.IGNORECASE)

    # Find those marked with duplicate string
    dup_files = []
    for my_file in all_files:
        basename = os.path.basename(my_file)
        dir_name = os.path.dirname(my_file)
        if p.match(basename):
            # Now, turn the duplicated files into their original file names by
            # removing the duplicate string.
            orig_file = basename.replace(duplicate_str, "")
            orig_file = os.path.join(dir_name, orig_file)

            # Check that this original file exists
            if orig_file in all_files:
                # If yes, add the two files as a tuple to the list
                dup_files.append((orig_file, my_file))
            else:
                print("Failed to find original file for '%s'" % basename)

    return dup_files


def fix(data_dir, dup_str):
    """Find and fix duplicate CSV files from subsequent API calls.

    This method reads the given data directory for files exhibiting the
    duplicate string (``dup_str``) and its original file name (i.e., without)
    the duplicate string, analyzes the two files for data gaps, and attempts
    to merge the two files together to form a more complete dataset.

    Parameters
    ----------
    data_dir : str
        The folder path to CSV files.
    dup_str : str
        The string used to distinguish original and duplicated CSV files.
    """
    dup_files = find_duplicate_archives(data_dir, dup_str)
    for dup_pair in dup_files:
        # The goal is to merge these two datasets, drop any duplicates, and
        # see if the resulting file is "more complete" than before.
        # NOTE: the duplicate has the original file name (latest API run) and
        # the original has the dup string (i.e., the archive).
        dup_file, orig_file  = dup_pair
        orig_df = pd.read_csv(orig_file)
        dup_df = pd.read_csv(dup_file)
        print("Correcting %s" % os.path.basename(dup_file))

        # Pull stats from our two files
        o_lines, o_miss, o_months = analyze_df(orig_df)
        d_lines, d_miss, d_months = analyze_df(dup_df)

        # If the latest API run fixed the problem, then we're good here.
        # UPDATE: add line equivalence; new data may be within the same month.
        if d_miss == 0 and o_lines == d_lines:
            print("\tNo missing months!")
            continue

        # Here, we will assume the same column names in the original and dup
        # file. You could confirm this as a measure of confidence.
        # I'm choosing to skip the existence check, since this method assumes
        # you already ran :func:`run` without errors.
        date_col = find_column(orig_df, 'date')

        hr_col = find_column(orig_df, 'hour')
        fac_col = find_column(orig_df, 'facility_name')

        # Sort order is each facility's time series
        sort_cols = []
        if fac_col:
            sort_cols.append(fac_col)
        if date_col:
            sort_cols.append(date_col)
        if hr_col:
            # Add the hourly column for sorting hourly data
            sort_cols.append(hr_col)

        # Choose to overwrite ``orig_df`` as a memory-saving device; rather than
        # create yet another variable in memory. The downside is if we need to
        # reference the original again.
        orig_df = pd.concat([orig_df, dup_df], ignore_index=True)

        # The ISO string format for dates means lexicographic sorting is also
        # chronologic. Yay!
        orig_df = orig_df.sort_values(by=sort_cols, ascending=True)

        # Remove duplicates after sorting
        orig_df = orig_df.drop_duplicates()
        f_lines, f_miss, f_months = analyze_df(orig_df)

        # Compute percent missing months reduced and percent data lines
        # increased.
        pmmr = 100*(f_miss - o_miss)/o_miss
        pdli = 100*(f_lines - o_lines)/o_lines

        print(
            "\tMissing months from %d to %d (%0.1f%%)" % (o_miss, f_miss, pmmr)
        )
        print(
            "\tData lines from %d to %d (%0.1f%%)" % (o_lines, f_lines, pdli)
        )

        # Recall that ``dup_file`` has the original filename (i.e., without
        # the duplicate string, 'ABCD', in it). Save the fixed data frame to the
        # original file name.
        print("Overwriting %s" % dup_file)
        orig_df.to_csv(dup_file, index=False)


# NEW
def query_epa_cams(year,
                     month,
                     state,
                     api_key="",
                     period="daily",
                     to_save=False,
                     time_out=60,
                     api_wait=3.6):
    """Helper function to archive EPA's daily and hourly CEMS data for a given
    state, month, and year.

    Parameters
    ----------
    year : int
        The year to process (e.g., 2022).
    month : int
        The month to process (e.g., 1 for January).
    state : str
        The two-character state abbreviation (e.g., 'PA' for Pennsylvania).
    api_key : str, optional
        Your personal EPA CAMPD API key (prompt for input if not provided), by default ""
    period : str, optional
        One of three time periods to archive (options include: 'annual', 'daily' and 'hourly'), by default "daily"
    to_save : bool, optional
        Whether to write the data frame to CSV in eLCI output directory.
        Defaults to false.
    time_out : int, optional
        The timeout (in seconds) to wait for an API response.
        API may take longer to respond for 'hourly' than for 'annual' requests.
    api_wait : float, optional
        The sleep time (in seconds) to wait in-between API calls as a courtesy
        to EPA's servers. Defaults to 3.6 s.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
        If the time period or state provided is not one of the valid options

    Notes
    -----
    Heavily derived from :func:`archive_epa_cams` in ElectricityLCI's utils.py.
    The purpose of this method is to gap-fill single months for a given state.

    Test the API out `here <https://campd.epa.gov/data/custom-data-download>`_
    """
    # Needs these globals and utilities
    from electricitylci.cems_data import CEMS_STATES
    from electricitylci.globals import CAM_API_URL
    from electricitylci.utils import check_api
    from electricitylci.utils import next_month
    from electricitylci.utils import read_from_api
    from electricitylci.utils import write_csv_to_output

    # API max retries parameter
    max_tries = 4

    # Check that the user provided a valid API key
    cam_api = "https://www.epa.gov/power-sector/cam-api-portal#/api-key-signup"
    api_key = check_api(api_key, "EPA", cam_api)

    # Check that the user selects a valid period
    valid_cams_periods = ['hourly', 'daily']
    if period not in valid_cams_periods:
        warn_msg = (
            "Expected, '%s', received '%s'" % (
                ", ".join(valid_cams_periods), period
            )
        )
        raise ValueError(warn_msg)

    if state not in CEMS_STATES:
        raise ValueError("The state, '%s', is not valid!" % state)

    # Column naming scheme to be consistent across datasets.
    c_map = {
        'stateCode': 'state',
        'facilityName': 'facility_name',
        'facilityId': 'plant_id_eia',
        'year': 'year',
        'grossLoad': 'gross_load_mwh',
        'steamLoad': 'steam_load_1000_lbs',
        'so2Mass': 'so2_mass_tons',
        'co2Mass': 'co2_mass_tons',
        'noxMass': 'nox_mass_tons',
        'heatInput': 'heat_content_mmbtu'
    }
    # Columns to check for data (row dropped if all entries are NaN)
    data_cols = [
        'gross_load_mwh',
        'steam_load_1000_lbs',
        'so2_mass_tons',
        'co2_mass_tons',
        'nox_mass_tons',
        'heat_content_mmbtu',
    ]

    # Create the new API URL
    cam_url = CAM_API_URL.replace("/annual/", f"/{period}/")

    # Run for one month
    start_date = datetime.date(year, month, 1)
    end_date = next_month(start_date) - datetime.timedelta(days=1)

    # Prepare the empty data frame
    df = pd.DataFrame(columns=list(c_map.values()))

    # Initialize variables to start the API query for all records.
    recs_received = 0
    recs_total = 2 # needs to >1 to initiate the loop
    page_no = 1
    while recs_received < (recs_total - 1):
        # Build the params; the page number will increment
        params = {
            'api_key': api_key,
            'beginDate': start_date.isoformat(),
            'endDate': end_date.isoformat(),
            'stateCode': state,
            'page': page_no,
            'perPage': 500,  # max allowable by API is 500
        }

        # Query the API; url_tries will max with no data upon failing
        try:
            js_list, url_tries, h_dict = read_from_api(
                cam_url,
                params=params,
                max_tries=max_tries,
                time_out=time_out
            )
        except Exception as e:
            # Hitting urllib3 and requests errors; just kill this state
            js_list = []  # add zero to recs received
            h_dict = {}   # set total recs to zero
            url_tries = max_tries # set success to false

        # EPA's rate limit is 1000 requests per hour.
        # This limits you to 3.6 seconds per request to avoid exceeding.
        # The API may recommend a different wait time.
        # Daily data has roughly 12k records per state; with 49 states,
        # that's ~600k records; that's 1200 requests, which is more than
        # the 1000 per hour rate limit, so let's impose the 3.6s wait,
        # unless otherwise specified
        sleep_time = h_dict.get("Retry-After", api_wait)
        sleep_time = float(sleep_time)
        time.sleep(sleep_time)

        # update the total records and received records
        recs_total = h_dict.get('X-Total-Count', 0)
        recs_total = int(recs_total)
        recs_received += len(js_list)

        tmp_df = pd.DataFrame.from_dict(js_list).rename(columns=c_map)

        # HOTFIX: it may be valid for a month to have no data.
        # only skip if API fails
        # If no data or API failed, stop the query (incomplete data)
        if len(tmp_df) == 0 and url_tries < max_tries:
            print("No data for this query (page=%d)!" % page_no)
        elif len(tmp_df) == 0 and url_tries >= max_tries:
            print("Failed to retrieve data for %s %s (page=%d)!" % (
                state, year, page_no)
            )
        elif len(df) == 0 and len(tmp_df) > 0:
            # First time, set df
            df = tmp_df.copy()
        else:
            # We've been here before. We're going in circles, Sam!
            # NOTE: columns with all NaNs will raise a FutureWarning
            df = pd.concat([df, tmp_df], ignore_index=True)

        # Communicate where we are.
        print(
            "Received %d out of %d records (page=%d)" % (
                recs_received, recs_total, page_no
            )
        )

        # Increment page to continue
        page_no += 1

    # NOTE: decision here is to save only the rows that have data.
    # Rows with NaN values in all data columns are dropped.
    # If you favor a more complete time series (with data gaps), then
    # comment this line out.
    df = df.dropna(subset=data_cols, how='all')

    # Write to electricitylci's output folder.
    if len(df) > 0 and to_save:
        # Define the state-level CEMS data file
        archive_file = "epacems_%s_%d-%02d_%s.csv" % (
            period, year, month, state.lower()
        )
        write_csv_to_output(archive_file, df)
    elif to_save:
        print("Failed to write to CSV!")

    return df


def run(data_dir, year=None, freq=None):
    """Analyze EPA CAMPD hourly and daily CSV files for data gaps.

    Prints to console each CSV file found, the number of lines read, and
    whether any months were not reported (including a list of month integers
    where no data were identified).
    """
    # Find the EPA CAMPD CSV files based on the parameters
    my_glob = build_glob(data_dir, year, freq)
    my_files = glob.glob(my_glob)
    num_files = len(my_files)

    if num_files == 0:
        print("No files found for %d '%s' in '%s'!" % (year, freq, data_dir))
        return None

    # Initialize the total lines read
    tot_lines = 0

    for my_file in my_files:
        f_name = os.path.basename(my_file)
        my_data = pd.read_csv(my_file)
        lines_read, mos_missed, mos_list = analyze_df(my_data)
        if lines_read is None:
            print("Failed to find date column in file, '%s'!" % f_name)
        else:
            # Increment total lines read.
            tot_lines += lines_read
            if mos_missed > 0:
                print(
                    "Missing %d months %s (%s)" % (mos_missed, mos_list, f_name)
                )
            else:
                print("%s,%d" % (f_name, lines_read))

    print("Read %d lines from %d files" % (tot_lines, num_files))


##############################################################################
# MAIN
##############################################################################
if __name__ == '__main__':
    # Basic parameter definitions
    home_dir = os.path.expanduser("~")
    data_dir = os.path.join(home_dir, "Workspace", "data", "campd")
    year = None
    freq = "hourly"

    # Analyze the CSV data
    run(data_dir, year, freq)

    # Fix files after running the API a second time
    fix(data_dir, 'ABCD')
