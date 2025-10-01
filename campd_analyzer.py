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

TODO:

-   There may be an instance of running :func:`archive_epa_cams` multiple times
    in an attempt to create a complete time series (e.g., months Jan. and Feb.
    were unsuccessful in the first API call and months Sep. and Dec. failed
    to return data in a second API call). It may be possible to merge these
    two datasets together. The function, :func:`find_duplicate_archives` was
    created to identify pairs of CSV files (an original and a duplicate) based
    on a user's naming scheme (e.g., by adding 'ABCD' to one of the CSV's file
    name). The goal is create a method that finds all duplicated pairs, reads
    both, merged their content, drops duplicates, sorted by date, and writes
    back to CSV in an attempt to create complete CSV files that will not trip
    the :func:`run` method.

Author:
    Tyler W. Davis

Last updated:
    2025-10-01
"""


##############################################################################
# FUNCTIONS
##############################################################################
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
        A list of tuples. Each tuple is length two: original file path and its
        duplicated file path.
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

        # Two goals: count lines and make sure all months are present
        num_lines = len(my_data)
        tot_lines += num_lines

        date_col = [x for x in my_data.columns if x.lower() == 'date']
        if len(date_col) == 1:
            date_col = date_col[0]
        else:
            print("Failed to find date column in '%s'" % my_file)
            continue

        # Unique list of months (e.g., 1, 2, 10, 12)
        months = np.unique(
            [extract_year_month(x)[1] for x in my_data['date'].values]
        )
        # Run two checks on list of months
        num_months = len(months)
        sum_months = months.sum()
        if num_months == 12 and sum_months == 78:
            print("%s,%d" % (f_name, num_lines))
        elif num_months != 12:
            all_months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
            missed_mos = [x for x in all_months if x not in months]
            num_missed = len(missed_mos)
            print(
                "Missing %d months %s (%s)" % (num_missed, missed_mos, f_name)
            )
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
    run(data_dir, year, freq)
