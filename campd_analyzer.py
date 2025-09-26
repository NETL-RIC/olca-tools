#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# campd_analyzer.py
#
# This module reads the daily and hourly EPA CAMPD archive CSV files to
# count the lines archived and check for completeness (a simple review
# of months found in a dataset, based on how the `archive_epa_cams` function
# in ElectricityLCI was written, which queries one month at a time). The
# number of lines read from each file and the total lines read are printed
# to console. Any files that have missing months are printed with a 'Missing'
# statement.
#
# Author: Tyler W. Davis
# Last updated: 2025-09-26
#
##############################################################################
# REQUIRED IMPORTS
##############################################################################
import datetime
import glob
import os

import numpy as np
import pandas as pd


##############################################################################
# FUNCTIONS
##############################################################################
def build_glob(data_dir, year=None, freq=None):
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


def run(data_dir, year=None, freq=None):
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
    year = 2024
    freq = "hourly"
    run(data_dir, None, freq)
