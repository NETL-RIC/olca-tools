#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# elci_to_rem.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import argparse
import logging
import os
from zipfile import ZipFile
from zipfile import ZIP_DEFLATED

import geopandas as gpd
import pandas as pd
import requests


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This software is a "United States Government Work" under the
terms of the United States Copyright Act. It was written as part of the
authors' official duties as a United States Government employee and thus
cannot be copyrighted. This software is freely available to the public for
use. National Energy Technology Laboratory (NETL) have relinquished control
of the information and no longer have responsibility to protect the integrity,
confidentiality, or availability of the information provided herein.

This module is designed to run as a command-line interface (CLI) tool. This
is particularly true when multiple years of residual mixes are to be generated.
The challenge is using eLCI over multiple years, which requires a kernel restart
due to how configuration data are stored in memory.

This module contains four global parameters:

-   DATA_DIR (str), the output data directory
-   GREEN_E (list), the primary fuel categories associated with renewable energy
-   OVERFLOW_E (list), the primary fuel categories that could have renewable
    energy associated with them (i.e., broad categories)
-   ELCI_LOADED (bool), a tracker for if/when ElectricityLCI package is loaded.

The main method for calculating the residual grid mixes is :func:`run`, which
calls :func:`get_elci_mix` to get the generation mix for a given year based on
EIA Form 923, and :func:`update_mix` to reduce the renewable energy generation
based on the REC sales as published in NREL's Status and Trends in the U.S.
Voluntary Green Power Market Excel workbook[1].

Command-line arguments include how the state-level REC amounts are aggregated
to the balancing authority area. Two methods are available:

1.  Areal weighting, which uses geospatial analysis (provided by geopandas)
    to calculate the fraction of each state covered by balancing authorities
    (i.e., using a spatial overlay).
2.  Facility count weighting, which uses regional information provided in
    EIA Form 860 to calculate the relative number of facilities in each
    state that are also located in a balancing authority area.

The second argument is how negative renewable energy generation is handled.
Due to the categorization of renewables (e.g., wind, solar, and hydro),
it is possible that REC generation is greater than the "green" electricity
generation provided by a balancing authority. When this happens, there are
two ways of handling it:

1.  Keep the negative "green" generation amounts, and subtract the excess
    from the non-renewable portion of generation. This assumes that some of
    the non-renewable fuel categories (e.g., "mix" or "othf") compensate for
    the difference.
2.  Zero the green energy. Excess remains unaccounted.

The recommended (and default) options are to use the "zero" accounting and
"facility count" aggregation methods.

Note that this module may create up to three files that are, by default,
saved in a local "data" folder. These files are:

1.  cb_2020_us_state_500k.zip
    - An Esri shapefile of U.S. state boundaries.
    - Created when calling :func:`get_state_geo`
2.  control_areas.geojson
    - A GeoJSON file from HIFLD of U.S. balancing authority areas
    - Created when calling :func:`get_ba_geo`
3.  nrel-green-power-data-v2023.xlsx
    - An Excel workbook from NREL of 2016-2023 REC generation totals
    - Created when calling :func:`get_rec`

This module, main.py, includes a command-line interface and supports a series
of arguments that may be passed as configuration parameters for running the
module. The usage and arguments are as follows::

    usage: main.py [-h] [-f REC_FILE] [-r {keep,zero}] [-a {area,count}] [-v]
                [-s] [-l {NOTSET,DEBUG,INFO,WARNING,ERROR}]

    The residual grid mix Python tool.

    optional arguments:
    -h, --help
        show this help message and exit
    -f REC_FILE, --rec_file REC_FILE
        path to NREL Green Power Data Excel workbook
    -r {keep,zero}, --rec_method {keep,zero}
        method for managing negative renewable generation
    -a {area,count}, --agg_method {area,count}
        method for aggregating REC generation from states to BA areas
    -y YEAR, --year YEAR
        generation year (e.g., 2016 or 2020)
    -v, --verbose
        print results to console
    -s, --save
        write results to CSV file
    -l, --log_level {NOTSET,DEBUG,INFO,WARNING,ERROR}
        set logging level, defaults to INFO

References

1.  E. O'Shaughnessy, S. Jena, and D. Salyer. 2024. Status and Trends in the
    Voluntary Market (2023 Data). Golden, CO: NREL. Online:
    https://www.nrel.gov/docs/libraries/analysis/nrel-green-power-data-v2023.xlsx


Examples:
>>> python main.py -s # run defaults and save to CSV file
>>> python main.py -f "./data/nrel-green-power-data-v2023.xlsx" -v
>>> python main.py -a "count" -r "zero" -y 2016 -s

Version:
    2.0.1
Last Edited:
    2025-10-03
Changelog:
    -   Version 2 publication: https://doi.org/10.18141/2503966
"""


##############################################################################
# GLOBALS
##############################################################################
DATA_DIR = "data"
'''str: Local directory for storing data files.'''
GREEN_E = ['HYDRO', 'BIOMASS', 'SOLAR', 'SOLARTHERMAL', 'WIND', 'GEOTHERMAL']
'''list: Green or renewable energy categories.'''
OVERFLOW_E = ['MIXED', 'OTHF']
'''list: Non-renewable fuel categories that can lend overflow electricity.'''
ELCI_LOADED = False
'''bool: Tracker for if/when to load ElectricityLCI package.'''


##############################################################################
# FUNCTIONS
##############################################################################
def agg_by_area(year, rec_file=None):
    """Partition state-based REC electricity using the relative areal fraction
    that each state is covered by a balancing authority.

    Notes
    -----
    1.  The areal coverage between states and balancing authority areas is
        not 1:1---there are overlaps and gaps---therefore, relative coverage
        is used in place of actual coverage.
    2.  The geospatial data frames for states and balancing authority areas
        are in GCS, so project! A reasonable projection for continental U.S.
        is North America Lambert Conformal Conic (ESRI:102009).

    Parameters
    ----------
    year : int
        The year for REC sales data.
    rec_file : str, optional
        A filepath (i.e., absolute/relative path to an existing file) to the
        the NREL Green Power Excel workbook (see :func:`get_rec`),
        by default None, which triggers method to retrieve file from the web.

    Returns
    -------
    pandas.Series
        A Pandas series where the index is 'BA_CODE' (balancing authority
        area abbreviation) and the values are 'REC_FRAC' (allocated REC
        generation from states to their BA areas).
    """
    logging.info("Aggregating by area for year %d" % year)
    # Get geospatial dataframes and project to 2D
    ba_df = get_ba_geo(correct_names=True)
    us_df = get_state_geo(year)
    pcs = ('esri', 102009)
    ba_df = ba_df.to_crs(pcs)
    us_df = us_df.to_crs(pcs)
    # Preserve original state and BA areas
    ba_df['BAA_KM2'] = ba_df['geometry'].area / 10**6
    us_df['ST_KM2'] = us_df['geometry'].area / 10**6
    area_df = gpd.overlay(ba_df, us_df, how='intersection')
    # Calculate the area of the overlaps in square kilometers:
    area_df['AREA_KM2'] = area_df['geometry'].area / 10**6

    # Calculate fractional coverage
    area_df['BA_FRAC'] = area_df['AREA_KM2'] / area_df['BAA_KM2']
    area_df['ST_FRAC'] = area_df['AREA_KM2'] / area_df['ST_KM2']

    # For each BA+state, find the state areas and their fractional coverage
    tmp_df = area_df.groupby(
        by=['BA_CODE', 'STUSPS'])[['AREA_KM2', 'BA_FRAC', 'ST_FRAC']].agg('sum')
    tmp_df.index.names = ["BA_CODE", "STATE_ABBR"]
    tmp_df.reset_index(drop=False, inplace=True)

    # Convert absolute state fractions to relative fractions (normalize)
    #   after this, summing ST_FRAC for each state should give 1.0
    for my_st in tmp_df['STATE_ABBR'].unique():
        my_rows = tmp_df['STATE_ABBR'] == my_st
        max_val = tmp_df.loc[my_rows, 'ST_FRAC'].sum()
        tmp_df.loc[my_rows, 'ST_FRAC'] /= max_val

    # Table join REC totals to their respective states
    rec_df = get_rec(year, rec_path=rec_file)
    rec_df = rec_df[['State', 'Total']].copy()
    rec_df.rename(
        columns={'State': "STATE_ABBR", 'Total': "REC_GEN"}, inplace=True)
    jdf = tmp_df.merge(rec_df, how='left', on='STATE_ABBR')

    # Calculate REC generation based on relative state area fractions
    # NOTE: 'sum' on REC_GEN keeps original value (no summing)
    tmp_df = jdf.groupby(
        by=['STATE_ABBR', 'BA_CODE'])[['ST_FRAC', 'REC_GEN']].agg('sum')
    tmp_df['REC_FRAC'] = tmp_df['ST_FRAC'] * tmp_df['REC_GEN']

    # Drop indices for easier handling
    tmp_df.reset_index(drop=False, inplace=True)

    # Allocate RECs to their BA areas using the new relative fractions
    # The sum of REC_FRAC should equal the sum of Total in the REC data frame
    tot_df = tmp_df.groupby(by='BA_CODE')['REC_FRAC'].agg("sum")
    return tot_df


def agg_by_count(year, rec_file=None):
    """Partition state-based REC electricity generation using the fractional
    weights of electricity generating facility counts found within the shared
    boundaries of each state and balancing authority area.

    Notes
    -----
    This method assumes the state names reported in EIA Form 860 (i.e., this
    does not perform any spatial analysis).

    Parameters
    ----------
    year : int
        The year for REC sales data.
    rec_file : str, optional
        A filepath (i.e., absolute/relative path to an existing file) to the
        the NREL Green Power Excel workbook (see :func:`get_rec`),
        by default None, which triggers method to retrieve file from the web.

    Returns
    -------
    pandas.Series
        A Pandas series where the index is 'BA_CODE' (balancing authority
        area abbreviation) and the values are 'REC_FRAC' (allocated REC
        generation from states to their BA areas).
    """
    logging.info("Aggregating by state counts for year %d" % year)
    ba_df = get_ba_plants(year)
    ba_df.rename(columns={
        'Balancing Authority Name': "BA_NAME",
        'Balancing Authority Code': "BA_CODE"}, inplace=True)

    # Not every plant has a BA and State, so drop NAs
    ba_df = ba_df.dropna(subset='BA_CODE')

    # Create plant count tables
    table_1 = ba_df.value_counts(subset=['State', 'BA_CODE'])
    table_1.name = "STBA_PLANTS"
    table_2 = ba_df.value_counts(subset=['State'])
    table_2.name = "ST_PLANTS"

    # Join these series together
    t1_df = table_1.reset_index(drop=False)
    t2_df = table_2.reset_index(drop=False)
    t1_df = t1_df.merge(t2_df, how='left', on='State')

    # Calculate state-level fractions
    # these should all add to 1.0 given the NA drop above.
    t1_df["ST_FRAC"] = t1_df['STBA_PLANTS'] / t1_df['ST_PLANTS']

    # Join REC data
    rec_df = get_rec(year,rec_path=rec_file)
    rec_df = rec_df[['State', 'Total']].copy()
    jdf = t1_df.merge(rec_df, how='left', on='State')

    # Calculate BA REC amounts using plant count fractions
    jdf['REC_FRAC'] = jdf['ST_FRAC'] * jdf['Total']

    # Allocate RECs to their BA areas using the new relative fractions
    # the sum of REC_FRAC should equal the sum of Total in the REC data frame
    tot_df = jdf.groupby(by='BA_CODE')['REC_FRAC'].agg("sum")
    tot_df.index.names = ['BA_CODE']
    return tot_df


def calc_relative_ratio(df, add_total=False):
    """Calculate the relative electricity generation fractions in a data frame.

    Parameters
    ----------
    df : panda.DataFrame
        A data frame with 'Electricity' field for electricity per fuel category.
        For example, taking a subset of electricityLCI's generation mix for
        a given Balancing Authority area.
    add_total : bool, optional
        Switch to include 'Relative_Total' field, by default False

    Returns
    -------
    pandas.DataFrame
        The same as the argument, but with new fields, "Relative_Ratio"
        and optional "Relative_Total".`

    Raises
    ------
    TypeError
        When argument object is not a data frame.
    IndexError
        When data frame does not have the expected 'Electricity' field.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected data frame, received %s" % type(df))
    if "Electricity" not in df.columns:
        raise IndexError("Data frame missing required 'Electricity' field!")
    total_e = df['Electricity'].sum()
    df['Relative_Ratio'] = df['Electricity'] / total_e
    if add_total:
        df['Relative_Total'] = total_e
    return df


def correct_ba_geo_names(ba_geo_df):
    """Create new named column, 'BA_NAME', with mapped balancing authority
    names from HILD geospatial dataset to 2020 EIA Form 860 names.

    Notes
    -----
    Not all 2021 balancing authority names match the 2020 EIA Form 860 names
    and not all EIA Form 860 balancing authorities are represented in the HILD
    geo dataset (e.g., Hawaiian and Canadian authorities).

    The corrections are not as simple as making the names title case (e.g.,
    LLC, JEA, and AVBA); also, some uncertainty remains with current matches,
    such as 'Salt River Project' and 'NorthWestern Corporation'.

    Mix names that are unmatched in the geo data frame, include the following.

    -  'B.C. Hydro & Power Authority'
    -  'Hydro-Quebec TransEnergie'
    -  'Manitoba Hydro'
    -  'Ontario IESO'

    An alternative would be to match all BA areas to their representative
    BA codes, which there exists code for doing just that in scenario modeler's
    BA class.

    Parameters
    ----------
    ba_geo_df : geopandas.GeoDataFrame
        The geospatial data frame created in :func:`get_ba_geo`.

    Returns
    -------
    geopandas.GeoDataFrame
        The same as the input data frame with a new mapped column, 'BA_NAME'.
    """
    m_dict = {
        'NEW BRUNSWICK SYSTEM OPERATOR': (
            'New Brunswick System Operator'),
        'POWERSOUTH ENERGY COOPERATIVE': (
            'PowerSouth Energy Cooperative'),
        'ALCOA POWER GENERATING, INC. - YADKIN DIVISION': (
            'Alcoa Power Generating, Inc. - Yadkin Division'),
        'ARIZONA PUBLIC SERVICE COMPANY': (
            'Arizona Public Service Company'),
        'ASSOCIATED ELECTRIC COOPERATIVE, INC.': (
            'Associated Electric Cooperative, Inc.'),
        'BONNEVILLE POWER ADMINISTRATION': (
            'Bonneville Power Administration'),
        'CALIFORNIA INDEPENDENT SYSTEM OPERATOR': (
            'California Independent System Operator'),
        'DUKE ENERGY PROGRESS EAST': (
            'Duke Energy Progress East'),
        'PUBLIC UTILITY DISTRICT NO. 1 OF CHELAN COUNTY': (
            'Public Utility District No. 1 of Chelan County'),
        'CHUGACH ELECTRIC ASSN INC': (
            'Chugach Electric Assn Inc'),
        'PUD NO. 1 OF DOUGLAS COUNTY': (
            'PUD No. 1 of Douglas County'),
        'DUKE ENERGY CAROLINAS': (
            'Duke Energy Carolinas'),
        'EL PASO ELECTRIC COMPANY': (
            'El Paso Electric Company'),
        'ELECTRIC RELIABILITY COUNCIL OF TEXAS, INC.': (
            'Electric Reliability Council of Texas, Inc.'),
        'ELECTRIC ENERGY, INC.': (
            'Electric Energy, Inc.'),
        'FLORIDA POWER & LIGHT COMPANY': (
            'Florida Power & Light Co.'),
        'DUKE ENERGY FLORIDA INC': (
            'Duke Energy Florida, Inc.'),
        'GAINESVILLE REGIONAL UTILITIES': (
            'Gainesville Regional Utilities'),
        'CITY OF HOMESTEAD': (
            'City of Homestead'),  # fixed for eLCIv2
        'IDAHO POWER COMPANY': (
            'Idaho Power Company'),
        'IMPERIAL IRRIGATION DISTRICT': (
            'Imperial Irrigation District'),
        'JEA': (
            'JEA'),
        'LOS ANGELES DEPARTMENT OF WATER AND POWER': (
            'Los Angeles Department of Water and Power'),
        'LOUISVILLE GAS AND ELECTRIC COMPANY AND KENTUCKY UTILITIES': (
            'Louisville Gas and Electric Company and Kentucky Utilities '
            'Company'), # fixed for eLCIv2
        'NORTHWESTERN ENERGY (NWMT)': (
            'NorthWestern Corporation'),
        'NEVADA POWER COMPANY': (
            'Nevada Power Company'),
        'ISO NEW ENGLAND INC.': (
            'ISO New England'),  # fixed for eLCIv2
        'NEW SMYRNA BEACH, UTILITIES COMMISSION OF': (
            'Utilities Commission of New Smyrna Beach'), # fixed for eLCIv2
        'NEW YORK INDEPENDENT SYSTEM OPERATOR': (
            'New York Independent System Operator'),
        'OHIO VALLEY ELECTRIC CORPORATION': (
            'Ohio Valley Electric Corporation'),
        'PACIFICORP - WEST': (
            'PacifiCorp West'),
        'PACIFICORP - EAST': (
            'PacifiCorp East'),
        'GILA RIVER POWER, LLC': (
            'Gila River Power, LLC'),
        'FLORIDA MUNICIPAL POWER POOL': (
            'Florida Municipal Power Pool'),
        'PUBLIC UTILITY DISTRICT NO. 2 OF GRANT COUNTY, WASHINGTON': (
            'Public Utility District No. 2 of Grant County, Washington'),
        'PJM INTERCONNECTION, LLC': (
            'PJM Interconnection, LLC'),
        'PORTLAND GENERAL ELECTRIC COMPANY': (
            'Portland General Electric Company'),
        'AVANGRID RENEWABLES LLC': (
            'Avangrid Renewables, LLC'), # fixed for eLCIv2
        'PUBLIC SERVICE COMPANY OF COLORADO': (
            'Public Service Company of Colorado'),
        'PUBLIC SERVICE COMPANY OF NEW MEXICO': (
            'Public Service Company of New Mexico'),
        'PUGET SOUND ENERGY': (
            'Puget Sound Energy, Inc.'),
        'BALANCING AUTHORITY OF NORTHERN CALIFORNIA': (
            'Balancing Authority of Northern California'),
        'SALT RIVER PROJECT': (
            'Salt River Project Agricultural Improvement and Power District'),
        'SEATTLE CITY LIGHT': (
            'Seattle City Light'),
        'SOUTH CAROLINA ELECTRIC & GAS COMPANY': (
            'Dominion Energy South Carolina, Inc.'), # fixed for eLCIv2
        'SOUTH CAROLINA PUBLIC SERVICE AUTHORITY': (
            'South Carolina Public Service Authority'),
        'SOUTHWESTERN POWER ADMINISTRATION': (
            'Southwestern Power Administration'),
        'SOUTHERN COMPANY SERVICES, INC. - TRANS': (
            'Southern Company Services, Inc. - Trans'),
        'CITY OF TACOMA, DEPARTMENT OF PUBLIC UTILITIES, LIGHT DIVISION': (
            'City of Tacoma, Department of Public Utilities, Light Division'),
        'CITY OF TALLAHASSEE': (
            'City of Tallahassee'), # fixed for eLCIv2
        'TAMPA ELECTRIC COMPANY': (
            'Tampa Electric Company'),
        'TENNESSEE VALLEY AUTHORITY': (
            'Tennessee Valley Authority'),
        'TURLOCK IRRIGATION DISTRICT': (
            'Turlock Irrigation District'),
        'HAWAIIAN ELECTRIC CO INC': (
            'Hawaiian Electric Co Inc'),
        'WESTERN AREA POWER ADMINISTRATION UGP WEST': (
            'Western Area Power Administration - Upper Great Plains West'),
        'AVISTA CORPORATION': (
            'Avista Corporation'),
        'SEMINOLE ELECTRIC COOPERATIVE': (
            'Seminole Electric Cooperative'),
        'TUCSON ELECTRIC POWER COMPANY': (
            'Tucson Electric Power'),
        'WESTERN AREA POWER ADMINISTRATION - DESERT SOUTHWEST REGION': (
            'Western Area Power Administration - Desert Southwest Region'),
        'WESTERN AREA POWER ADMINISTRATION - ROCKY MOUNTAIN REGION': (
            'Western Area Power Administration - Rocky Mountain Region'),
        'SOUTHEASTERN POWER ADMINISTRATION': (
            'Southeastern Power Administration'),
        'NEW HARQUAHALA GENERATING COMPANY, LLC - HGBA': (
            'New Harquahala Generating Company, LLC'), # fixed for eLCIv2
        'GRIFFITH ENERGY, LLC': (
            'Griffith Energy, LLC'),
        'NATURENER POWER WATCH, LLC (GWA)': (
            'NaturEner Power Watch, LLC'), # fixed for eLCIv2
        'GRIDFORCE SOUTH': (
            'Gridforce South'),
        'MIDCONTINENT INDEPENDENT TRANSMISSION SYSTEM OPERATOR, INC..': (
            'Midcontinent Independent System Operator, Inc.'),
        'ARLINGTON VALLEY, LLC - AVBA': (
            'Arlington Valley, LLC'), # fixed for eLCIv2
        'DUKE ENERGY PROGRESS WEST': (
            'Duke Energy Progress West'),
        'GRIDFORCE ENERGY MANAGEMENT, LLC': (
            'Gridforce Energy Management, LLC'),
        'NATURENER WIND WATCH, LLC': (
            'NaturEner Wind Watch, LLC'),
        'SOUTHWEST POWER POOL': (
            'Southwest Power Pool'),
    }
    ba_geo_df['BA_NAME'] = ba_geo_df['NAME'].map(m_dict)
    return ba_geo_df


def download_file(url, filepath):
    """Download a file from the web.

    Parameters
    ----------
    url : str
        A web address that points to a file.
    filepath : str
        A file path (including the file name) to where the local copy of the
        file should be downloaded.
    """
    r = requests.get(url)
    if r.ok:
        with open(filepath, 'wb') as f:
            f.write(r.content)


def get_elci_mix(gen_year=2016):
    """Create data frame of balancing authority electricity generation
    mix amounts by primary fuel category using EIA Form 860 and generation
    from EIA Form 923.

    Parameters
    ----------
    gen_year : int
        The year associated with the electricity generation (affects the
        data that are downloaded or accessed). Defaults to 2016.

    Returns
    -------
    pandas.DataFrame
        A data frame with fields: "Subregion" (i.e, balancing authority
        names), "FuelCategory" (i.e., primary fuel technology names),
        "Electricity" (i.e., annual generation, MWh), and "Generation_Ratio"
        (i.e., fraction of the total generation accounted by the fuel type
        for the given subregion). The data frame should be 332x4 if using
        electricitylci v.1.0.1.

        Now includes new field "BA_CODES" with balancing authority
        abbreviations based on `ba_codes` data frame found in the
        combinator module of electricitylci.

    Notes
    -----
    Warning if you attempt to run this method multiple times within the same
    Python instance as the EIA generation year does not correctly reset.
    For example, running this method with `gen_year` = 2016 followed by
    running this method with `gen_year` = 2020 without restarting the Python
    kernel, will have poor results (e.g., missing rows in dataset).
    As such, it is safest (and recommended) to run as a CLI. This is thanks to
    how eLCI stores its configuration in memory.
    """
    if not ELCI_LOADED:
        load_elci(gen_year)

    from electricitylci import get_generation_mix_process_df

    df = get_generation_mix_process_df(regions="BA")
    return map_ba_codes(df)


def get_ba_map():
    """Return a dictionary of balancing authority names and their abbreviations

    Notes
    -----
    Includes manual additions for GRIS, CEA, and HECO, which are found in
    the HIFLD data and not in the EIA balancing authority list.

    Parameters
    ----------
    year : int
        The year for eLCI generation data.

    Returns
    -------
    dict
        A dictionary with keys of balancing authority names (as per EIA 923)
        and values of abbreviations.
    """
    # NOTE: this module does not require model_specs :)
    from electricitylci.utils import read_ba_codes
    ba_codes = read_ba_codes()

    ba_map = {}
    for idx, row in ba_codes.iterrows():
        ba_map[row['BA_Name']] = idx

    # HOTFIX: add missing BA acronyms:
    ba_map['Gridforce South'] = 'GRIS'
    ba_map['Chugach Electric Assn Inc'] = 'CEA'
    ba_map['Hawaiian Electric Co Inc'] = 'HECO'

    return ba_map


def map_ba_codes(df):
    """Map balancing authority abbreviation codes based on EIA Form 930 naming.

    Parameters
    ----------
    df : pandas.DataFrame
        A data frame with column, 'Subregion' or 'BA_NAME' used to match
        against balancing authority abbreviation map.

    Returns
    -------
    pandas.DataFrame
        The same as the sent data frame with a new column, "BA_CODE".
    """
    m_col = 'Subregion'
    if 'Subregion' not in df.columns and 'BA_NAME' in df.columns:
        m_col = 'BA_NAME'
    elif 'Subregion' not in df.columns and 'BA_NAME' not in df.columns:
        logging.warning("No matching column for BA codes!")

    ba_map = get_ba_map()
    df['BA_CODE'] = df[m_col].map(ba_map)
    logging.info("%d mis-matched BA codes" % df['BA_CODE'].isna().sum())
    return df


def get_rec(year, rec_path=None, to_save=False):
    """Create state-level voluntary green power generation (MWh) data frame.

    Notes
    -----
    Data are based on the NREL Green Power Data by State (2013-2023)[1]_.
    Estimates are based on green power generated in each state, regardless of
    where the renewable energy certificate (REC) is retired.
    Some state-level totals do not add up to market-wide totals because some
    green power is purchased from Canada.

    There is an estimated 192.1 million MWh sold in 2020.

    [1] E. O'Shaughnessy, S. Jena, and D. Salyer. 2024. Status and Trends in
    the Voluntary Market (2023 Data). Golden, CO: NREL.

    See also
    --------
    1.  https://www.nrel.gov/analysis/green-power.html
    2.  https://data.nrel.gov/submissions/174

    Parameters
    ----------
    year : int
        The year for REC sales data.
    rec_path : str, optional
        A filepath (i.e., absolute/relative path to an existing file) to the
        referenced Excel workbook, by default None.
    to_save : bool, optional
        Switch, when set to true, saves a local copy of the Excel workbook to a
        "data" folder, by default False.

    Returns
    -------
    pandas.DataFrame
        A data frame with state-based green electricity generated (MWh).
        The "State" column provides the two-letter U.S. state name
        abbreviations and the "Total" column provides the total green
        electricity generated and sold as a REC in MWh.

        Other columns include:
        - "Year" (matches the year provided)
        - "Utility Green Pricing"
        - "Utility Renewable Contracts"
        - "Competitive Suppliers"
        - "Unbundled RECs"
        - "CCAs" (community choice aggregations)
        - "PPAs" (power purchase agreements)

    Examples
    --------
    >>> my_df = get_rec() # retrieve from URL
    >>> my_df = get_rec(to_save=True) # download a local copy
    >>> my_df = get_rec(rec_path="./data/NREL_Green_Power_Data_v2020.xlsx")
    """
    # If no file path given, either download Excel workbook or point to its URL
    if rec_path is None:
        rec_url = (
            "https://www.nrel.gov/"
            "docs/libraries/analysis/nrel-green-power-data-v2023.xlsx")
        # About File: ADDS a column, B, that includes the year! from 2016—> 2023 while prev only had 2020
        rec_name = os.path.basename(rec_url)
        rec_path = os.path.join(DATA_DIR, rec_name)

        if to_save:
            # Make sure output folder exists before attempting download:
            rec_dir = os.path.dirname(rec_path)
            if not os.path.isdir(rec_dir):
                os.mkdir(rec_dir)
            download_file(rec_url, rec_path)
        else:
            rec_path = rec_url

    # Sheet name, header, and index are based on examining the file.
    df = pd.read_excel(
        rec_path,
        sheet_name="State-Level Generation",
        header=4,
        index_col=None
    )
    df = df.loc[df["Year"]==year]
    return df


def get_ba_plants(year):
    """Return a data frame of electricity power plants and their region info.

    Notes
    -----
    Data are based on EIA Form 860 for the given year.

    Returns
    -------
    pandas.DataFrame
        A data frame with rows representing U.S. power plants.
        Columns include

        - 'Plant Id'
        - 'State' (two-letter abbreviation)
        - 'NERC Region'
        - 'Balancing Authority Code'
        - 'Balancing Authority Name'
    """
    logging.info("Pulling facility-level region info from eLCI for %d" % year)
    from electricitylci.eia860_facilities import eia860_balancing_authority
    df = eia860_balancing_authority(year=year)
    return df


def get_ba_geo(correct_names=False):
    """Create a geospatial data frame for U.S. control areas (i.e., balancing
    authorities).

    Run this method once to download a local copy of the GeoJSON.
    Subsequent runs of this method attempt to read the local file rather
    than re-download the file. The file name is "control_areas.geojson" and
    is saved in the DATA_DIR directory (e.g., ./data).

    Notes
    -----
    The API referenced in this method links to 2021 control areas, which were
    updated in 2022.

    When correcting BA names, there are a few that do not match the EIA 923
    names, which include Chugach Electric Assn Inc, Avangrid Renewables LLC,
    and Hawaiian Electric Co Inc.

    Source, "Control Areas" from Homeland Infrastructure Foundation Level
    Database (HIFLD). Online [1]_.

    [1] https://hifld-geoplatform.opendata.arcgis.com/datasets/geoplatform::control-areas/about

    Parameters
    ----------
    correct_names : bool, optional
        Whether to create a new named column, 'BA_NAME', with balancing
        authority names mapped to the EIA Form 860 balancing authority area
        names, defaults to false.

    Returns
    -------
    geopandas.geodataframe.GeoDataFrame
        A geospatial data frame of polygon areas representing the U.S.
        electricity control areas (i.e., balancing authorities).

        Columns include:

        - 'OBJECTID',
        - 'ID',
        - 'NAME',
        - 'ADDRESS',
        - 'CITY',
        - 'STATE',
        - 'ZIP',
        - 'TELEPHONE',
        - 'COUNTRY',
        - 'NAICS_CODE',
        - 'NAICS_DESC',
        - 'SOURCE',
        - 'SOURCEDATE',
        - 'VAL_METHOD',
        - 'VAL_DATE',
        - 'WEBSITE',
        - 'YEAR',
        - 'PEAK_MONTH',
        - 'AVAIL_CAP',
        - 'PLAN_OUT',
        - 'UNPLAN_OUT',
        - 'OTHER_OUT',
        - 'TOTAL_CAP',
        - 'PEAK_LOAD',
        - 'MIN_LOAD',
        - 'SHAPE__Area',
        - 'SHAPE__Length',
        - 'GlobalID',
        - 'geometry'
    """
    # NOTE: consider including comma-separated list of outFields, as not all
    # are needed and/or used.
    ba_api_url = (
        "https://services1.arcgis.com/"
        "Hp6G80Pky0om7QvQ/arcgis/rest/services/Control_Areas_gdb/"
        "FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=geojson")
    ba_file = "control_areas.geojson"
    ba_path = os.path.join(DATA_DIR, ba_file)

    # Check to make sure data directory exists before attempting download
    if not os.path.isdir(DATA_DIR):
        logging.info("Creating the data directory")
        os.mkdirs(DATA_DIR)

    # Use existing file if available:
    if not os.path.isfile(ba_path):
        logging.info("Downloading the balancing authority geoJSON file")
        download_file(ba_api_url, ba_path)

    # Read GeoJSON and correct BA area names (if requested)
    logging.info("Reading balancing authority spatial data")
    gdf = gpd.read_file(ba_path)
    if correct_names:
        gdf = correct_ba_geo_names(gdf)
        gdf = map_ba_codes(gdf)
    return gdf


def get_state_geo(year=2020, resolution="500k"):
    """Create geospatial data frame of U.S. state boundaries based on the
    Esri shapefiles provided by the U.S. Census Bureau.

    Run this method once to download a zipped copy of the Esri shapefile to
    your local machine. Subsequent runs will attempt to read the local zip
    file rather than re-download the file. The file name depends on the year
    provided. The default download location is the DATA_DIR (e.g., ./data).

    Parameters
    ----------
    year : int, optional
        Vintage for U.S. census state-level data, by default 2020
    resolution : str, optional
        The state boundary data resolution; valid options are the following in
        increasing quality: '2m' (1:20M), '5m' (1:5M), and '500k' (1:500k), by
        default "500k"

    Returns
    -------
    geopandas.geodataframe.GeoDataFrame
        Geospatial dataframe with polygon regions defining U.S. state boundaries. Columns include:

        - STATEFP (state-level FIPS code, two-digit)
        - STATENS
        - AFFGEOID
        - GEOID (same as STATEFP)
        - STUSPS (two-character state abbreviation)
        - NAME (state name)
        - LSAD
        - ALAND (land area)
        - AWATER (water area)
        - geometry

    Raises
    ------
    ValueError
        If year is given outside expected range or invalid resolution string.
    """
    if not (2014 <= year <= 2023): # works through 2023 now!
        raise ValueError("Expected year between 2014-2023, received %s" % year)
    if resolution not in ['5m', '20m', '500k']:
        raise ValueError(
            "Expected resolution of 20m, 5m, or 500k, "
            "and received '%s'" % resolution)

    base_url = f"https://www2.census.gov/geo/tiger/GENZ{year}/shp/"
    state_zip = f"cb_{year}_us_state_{resolution}.zip"
    zip_path = os.path.join(DATA_DIR, state_zip)
    shp_url = base_url + state_zip
    # Check to make sure data directory exists before attempting download
    if not os.path.isdir(DATA_DIR):
        os.mkdirs(DATA_DIR)
    # Use existing file if available:
    if not os.path.isfile(zip_path):
        download_file(shp_url, zip_path)
    return gpd.read_file(zip_path)


def get_rec_agg(year, agg_type, rec_path=None, as_series=True):
    """Return REC generation aggregated from states to balancing authority
    areas based on the aggregation type.

    Parameters
    ----------
    year : int
        The year associated with REC sales data.
    agg_type : str
        The aggregation type. Valid options are 'area' and 'count'.

        There are two options to determine the fraction of each state's
        RECs allocated to each balancing authority area.

        * The 'area' option performs normalized areal-weighting method
          between state and balancing authority boundaries.
        * The 'count' option uses facility counts that fall within the
          shared regions of states and balancing authorities.

    rec_path : str, optional
        A filepath (i.e., absolute/relative path to an existing file) to the
        referenced Excel workbook, by default None.
    as_series : bool, optional
        Switch to return object as either a Pandas series (if true) or as a
        data frame (if false), by default True.

    Returns
    -------
    pandas.Series or pandas.DataFrame
        A series or data frame with balancing authority codes (BA_CODE) and
        their REC generation amounts (REC_FRAC).

    Raises
    ------
    ValueError
        If aggregation type is not valid.
    """
    if agg_type == 'area':
        r = agg_by_area(year, rec_file=rec_path)
    elif agg_type == 'count':
        r = agg_by_count(year, rec_file=rec_path)
    else:
        raise ValueError(
            "Expected agg type to be either 'area' or 'count'; "
            "found '%s'" % agg_type)
    if not as_series:
        r = r.reset_index(drop=False)
    return r


def load_elci(year):
    """A helper method for dealing with ElectricityLCI and StEWI.

    Includes setting the configuration file based on the given year,
    updating the EIA generation year if the configuration file does not
    match the given year, updating the StEWI inventories of interest for
    the given year, and updating the model name for the given year.

    Parameters
    ----------
    year : int
        The year for eLCI data.

    Raises
    ------
    ValueError
        If the year provided is outside the valid range (>2015).
    """
    global ELCI_LOADED

    if ELCI_LOADED:
        logging.warning("eLCI is already loaded.")
    else:
        # Note: Choose correct config based on the given year
        if year >= 2016 and year < 2020:
            config_name = "ELCI_1"
        elif year == 2020:
            config_name = "ELCI_2020"
        elif year == 2021:
            config_name = "ELCI_2021"
        elif year >= 2022:
            config_name = "ELCI_2022"
        else:
            raise ValueError("Year, %d, is outside range (>2015)" % year)

        # Import necessary eLCI packages, the correct order, and set the
        # configuration for the generation year w/ renewables.
        from electricitylci import model_config as config
        config.model_specs = config.build_model_class(config_name)
        if config.model_specs.eia_gen_year != year:
            logging.info(
                "Updating EIA generation year from %s to %s" % (
                    config.model_specs.eia_gen_year, year))
            config.model_specs.eia_gen_year = year
            logging.info("Setting StEWI inventories of interest")
            config.model_specs.inventories_of_interest = get_stewi_invent_years(year)
            # HOTFIX: stop writing stewicombo files for bespoke models
            _model = "ELCI_%s" % year
            logging.info("Chaning eLCI model name from %s to %s" % (config.model_specs.model_name, _model))
            config.model_specs.model_name = _model
        config.model_specs.replace_egrid = True
        config.model_specs.include_renewable_generation = True
        ELCI_LOADED = True


def linear_search(lst, target):
    """Backwards search for the value less than or equal to a given value.

    Parameters
    ----------
    lst : list
        A list of numerically sorted data (lowest to highest).
    target : int, float
        A target value (e.g., year).

    Returns
    -------
    int
        The index of the search list associated with the value equal to or
        less than the target, else -1 for a target out-of-range (i.e., smaller than the smallest entry in the list).

    Examples
    --------
    >>> NEI_YEARS = [2011, 2014, 2017, 2020]
    >>> linear_search(NEI_YEARS, 2020)
    3
    >>> linear_search(NEI_YEARS, 2019)
    2
    >>> linear_search(NEI_YEARS, 2018)
    2
    >>> linear_search(NEI_YEARS, 2010)
    -1
    """
    for i in range(len(lst) - 1, -1, -1):
        if lst[i] <= target:
            return i
    return -1


def get_stewi_invent_years(year):
    """Helper function to return inventory years of interest from StEWI.
    See https://github.com/USEPA/standardizedinventories for inventory names
    and years.

    Parameters
    ----------
    year : int
        An inventory vintage (e.g., 2020).

    Returns
    -------
        dict
            A dictionary of inventory codes and their most recent year of
            data available (less than or equal to the year provided).

    Notes
    -----
    In eLCI, the inventories of interest configuration parameter cares about
    the following data providers,

    - eGRID
    - TRI
    - NEI
    - RCRAInfo

    Examples
    --------
    >>> get_stewi_invent_years(2019)
    """
    # A dictionary of StEWI inventories and their available vintages
    STEWI_DATA_VINTAGES = {
        # 'DMR': [x for x in range(2011, 2023, 1)],
        # 'GHGRP': [x for x in range(2011, 2023, 1)],
        'eGRID': [2014, 2016, 2018, 2019, 2020, 2021],
        'NEI': [2011, 2014, 2017, 2020],
        'RCRAInfo': [x for x in range(2011, 2023, 2)],
        'TRI': [x for x in range(2011, 2023, 1)],
    }

    r_dict = {}
    for key in STEWI_DATA_VINTAGES.keys():
        avail_years = STEWI_DATA_VINTAGES[key]
        y_idx = linear_search(avail_years, year)
        if y_idx != -1:
            r_dict[key] = STEWI_DATA_VINTAGES[key][y_idx]
    return r_dict


def run(rec_path=None, rec_handler='zero', agg_handler='count', gen_yr=2020,
        verbose=False, to_save=False):
    """A short-hand method for creating the residual grid mix data frame.

    Parameters
    ----------
    rec_path : str, optional
        A filepath (i.e., absolute/relative path to an existing file) to the
        referenced Excel workbook, by default None.
    rec_handler : str, optional
        Switch for handling REC totals that are greater than the renewable
        energy generated by a balancing authority. Valid options are 'keep'
        and 'zero'.

        - 'keep' will subtract the excess away from the non-renewable fuel
          categories (assuming some renewable generation in the 'mix' or 'othf'
          categories) and maintains the math of generation totals
        - 'zero' will floor negative renewable energy values to zero; the
          remainder is unaccounted for

    agg_handler : str, optional
        Switch for aggregation type. Valid options are 'area' and 'count'.

        * 'area' performs normalized areal-weighting method between state
          and balancing authority boundaries.
        * 'count' uses facility counts that fall within the shared regions
          of states and balancing authorities.

    gen_yr : int, optional
        The generation year to be replaced with residual mix values.
        Defaults to 2020, to match the REC data year.
    verbose : bool, optional
        Switch to print out results to console, by default false.
    to_save : bool, optional
        Switch to save results to CSV file, by default false.
        If true, output file is written to DATA_DIR in the format,
        "res-mix_[gen_yr]_rec-[rec_handler]_agg-[agg_handler].csv".

    Returns
    -------
    pandas.DataFrame
        A generation mix data frame with the following columns.

        - 'Subregion', the balancing authority name
        - 'FuelCategory', the primary fuel category (e.g., SOLAR, COAL)
        - 'Electricity', the annual generation for fuel category (MWh)
        - 'BA_CODE', the balancing authority abbreviation
        - 'Generation_Ratio', the mix fraction for the fuel category
        - 'Electricity_new', the REC-free annual generation by fuel (MWh)
        - 'Gen_Ratio_new', the REC-free mix fraction for fuel category
    """
    logging.info("Running residual grid mix calculation tool for %d" % gen_yr)
    m_df = get_elci_mix(gen_yr)
    m_df = update_mix(m_df, rec_path, rec_handler, agg_handler, gen_yr)
    logging.info("Complete!")

    if verbose:
        m_cols = list(m_df.columns)
        for _, row in m_df.iterrows():
            my_str = ",".join([str(row[i]) for i in m_cols])
            print(my_str)

    if to_save:
        logging.info("Writing data to file")
        out_file = f"res-mix_{gen_yr}_rec-{rec_handler}_agg-{agg_handler}.csv"
        out_path = os.path.join(DATA_DIR, out_file)
        save_csv(m_df, out_path)

    return m_df


def save_csv(data, fpath, to_zip=False):
    """Save data to CSV file.

    Parameters
    ----------
    data : str or pandas.DataFrame
        The data object.
    fpath : str
        The file path to where data should be saved.
    to_zip : bool, optional
        Whether to compress CSV. Defaults to False.

    Returns
    -------
    None

    Raises
    ------
    Exception
        All failed attempts to save data to file.

    Notes
    -----
    For pandas DataFrame-like objects, data frame indices are not included
    in the CSV file by default.

    Methods are based on scenario modeler's DataManager class.
    """
    logging.debug("Checking for output folder existence")
    out_dir = os.path.dirname(fpath)
    if not os.path.isdir(out_dir):
        logging.info("Creating output folder, '%s'" % out_dir)
        os.mkdir(out_dir)

    logging.debug("Writing data to file, '%s'" % fpath)
    if isinstance(data, str):
        if to_zip:
            if not fpath.endswith(".zip"):
                fpath += ".zip"
            try:
                with ZipFile(fpath,
                             'w',
                             compression=ZIP_DEFLATED,
                             compresslevel=3) as z:
                    z.write(data, arcname=os.path.basename(fpath))
            except:
                raise
            else:
                logging.debug("Saved data to zip.")
        else:
            try:
                with open(fpath, 'w') as f:
                    f.write(data)
            except:
                raise
            else:
                logging.debug("Saved data to CSV.")
    else:
        if to_zip:
            if not fpath.endswith('.zip'):
                fpath += ".zip"
            try:
                data.to_csv(
                    fpath, encoding="utf-8", compression="zip", index=False)
            except:
                raise
            else:
                logging.debug("Saved dataframe to zip.")
        else:
            try:
                data.to_csv(fpath, index=False, encoding="utf-8")
            except:
                raise
            else:
                logging.debug("Saved dataframe to CSV.")


def update_mix(df, rec_path=None, rec_handler='zero', agg_handler='count',
               year=2020):
    """Update a balancing authority generation mix by removing RECs.

    This methods appends two new columns to the generation mix data frame
    with REC-free electricity generation (MWh) and new mix fractions for
    each fuel category under each balancing authority.

    Notes
    -----
    -   Fixed mis-matched balancing authorities in 'count'-based REC aggregates
        by merging on BA_CODE field.
    -   Added check for non-green energy categories when dealing with overflow
        generation in REC data (as compared to the baseline generation).
        It should be further noted that the presence of one of these overflow
        fuel categories does not preclude non-renewable fuel categories (e.g.,
        coal, oil, or gas) from being reduced by overflowing REC-based
        generation. This would require a third bucket to be introduced to the
        model, such that overflow only comes from MIXED or OTHF categories.
        Currently all non-renewable categories are reduced to make up for the
        difference (when rec_handler is set to 'keep').

    See also
    --------
    The README for this repository includes the pseudocode and visual aids
    regarding this method. Variable names used in this method attempt to
    match the syntax of the pseudocode provided.

    Parameters
    ----------
    df : pandas.DataFrame
        Generation mix for balancing authorities.
        Required fields include 'Subregion', 'Electricity', 'Generation_Ratio',
        and 'FuelCategory'.
    rec_path : str, optional
        A filepath (i.e., absolute/relative path to an existing file) to the
        referenced Excel workbook, by default None.
    rec_handler : str, default 'zero'
        Switch for handling REC totals that are greater than the renewable
        energy generated by a balancing authority. There are two options.

        - 'keep' will subtract the excess away from the non-renewable fuel
          categories (assuming some renewable generation in the 'mix' or 'othf'
          categories) and maintains the math of generation totals
        - 'zero' will floor negative renewable energy values to zero; the
          remainder is unaccounted for

    agg_handler : str, optional
        Switch for aggregation type (see :func:`get_rec_agg`),
        by default 'count'.
    year : int
        The year associated with the generation mix and REC sales.

    Returns
    -------
    pandas.DataFrame
        The same as the generation mix data frame sent, but with two new
        fields.

        - 'Electricity_new' is the REC-free electricity generation amount (MWh)
          for each fuel category under each balancing authority area.
        - 'Gen_Ratio_new' is the REC-free fractional mix for each fuel category
          under each balancing authority area.

    Raises
    ------
    TypeError
        If the method does not receive a pandas data frame for df.
    IndexError
        If the pandas data frame, df, does not have the required fields.
    """
    # Basic error handling
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Expected a pandas data frame, found %s" % type(df))
    r_cols = ['Electricity', 'Generation_Ratio', 'Subregion']
    if not all([i in df.columns for i in r_cols]):
        raise IndexError("Data frame missing required columns!")
    if rec_handler not in ['keep', 'zero']:
        raise ValueError(
            "Must assign valid REC handler method. "
            "Options are 'keep' and 'zero', received '%s'" % rec_handler)
    elif rec_handler == 'keep':
        logging.info(
            "Negative renewable energy will be taken from "
            "non-renewable energy generation amounts.")
    elif rec_handler == 'zero':
        logging.info("Negative renewable energy will be zeroed.")

    # Initialize the new columns with existing electricity amounts and
    # generation ratios (e.g., for regions with no renewables, these values
    # shouldn't change).
    df['Electricity_new'] = df['Electricity']
    df['Gen_Ratio_new'] = df['Generation_Ratio']

    # Define columns used to merge new electricity and generation mix values
    # to our data frame (referenced in the for-loop below).
    m_cols = ['Subregion', 'FuelCategory', 'Electricity_new', 'Gen_Ratio_new']

    # Get the aggregation series and pair to BA area names
    # NOTE: name corrections for geo BA dataframe should fix any mis-matches
    logging.info("Using %s method" % agg_handler)
    agg_df = get_rec_agg(year, agg_handler, rec_path, as_series=False)

    for baa in df['Subregion'].unique():
        # ~~~~~~~~~~~~~~~~
        # NON-GREEN ENERGY
        # ~~~~~~~~~~~~~~~~
        # Find all non-green energy sources and set the non-green energy
        # total (ng) and the non-REC non-green energy total (ngx).
        ng_df = df.query(
            "(FuelCategory not in @GREEN_E) & (Subregion == @baa)").copy()
        ng = 0.0
        ngx = 0.0
        if len(ng_df) > 0:
            ng_df = calc_relative_ratio(ng_df, add_total=True)
            ng_df.drop(
                ['Electricity', 'Generation_Ratio'], axis=1, inplace=True)
            ng = ng_df['Relative_Total'].values[0]
            ngx = ng

        # ~~~~~~~~~~~~
        # GREEN ENERGY
        # ~~~~~~~~~~~~
        # Find only green energy fuels for the given BA area and initialize
        # the non-REC green energy total (gx), to zero.
        g_df = df.query(
            "(FuelCategory in @GREEN_E) & (Subregion == @baa)").copy()
        gx = 0.0

        # Skip BA areas with no green energy; nothing to do!
        if len(g_df) > 0:
            g_df = calc_relative_ratio(g_df, add_total=True)
            g_df.drop(['Electricity', 'Generation_Ratio'], axis=1, inplace=True)

            # Merge and keep index; thanks to Wouter Overmeire (2012)
            # https://stackoverflow.com/a/11982843
            g_df = g_df.reset_index().merge(
                agg_df, how='left', on='BA_CODE').set_index("index")

            # Pull values from data frame for green energy total (big_g)
            # and REC energy total (rec_t) and use them to calculate the
            # non-REC green energy (gx). NOTE: the relative total and
            # rec frac columns are constants, so it's safe to pull just
            # one value from the lot.
            big_g = g_df['Relative_Total'].values[0]
            rec_t = g_df['REC_FRAC'].values[0]
            gx = big_g - rec_t

            # NOTE: due to the categorization of "Green energy," there is a
            # good chance for negative green generation amounts.
            # 1. If we keep the negative amounts, when these are added back to
            #    the generation totals, we can assume that the "mix" or
            #    "other" fuels compensate ('keep' option); or
            # 2. We can zero out the negatives ('zero' option).

            # Check for negative green generation
            if rec_t > big_g:
                logging.info(
                    "Negative renewable energy for %s (%0.2e MWh)" % (baa, gx))
                has_ofe = any(
                    [i in OVERFLOW_E for i in ng_df['FuelCategory'].values])
                if rec_handler == 'keep' and has_ofe:
                    # Pull the "excess" electricity from non-green
                    gx = 0.0
                    ngx = ng - (rec_t - big_g)
                    # Don't let total generation go negative
                    ngx = max(0.0, ngx)
                else:
                    gx = 0.0

        # Calculate non-REC
        non_rec = gx + ngx

        # Calculate non-REC generation amounts and ratios of each fuel type
        if len(ng_df) > 0:
            ng_df['Electricity_new'] = ng_df['Relative_Ratio'] * ngx
            ng_df['Gen_Ratio_new'] = 0.0
            if non_rec > 0:
                ng_df['Gen_Ratio_new'] = ng_df['Electricity_new'] / non_rec
            df.update(ng_df[m_cols], join='left', overwrite=True)
        if len(g_df) > 0:
            g_df['Electricity_new'] = g_df['Relative_Ratio'] * gx
            g_df['Gen_Ratio_new'] = 0.0
            if non_rec > 0:
                g_df['Gen_Ratio_new'] = g_df['Electricity_new'] / non_rec
            df.update(g_df[m_cols], join='left', overwrite=True)
    return df


##############################################################################
# MAIN
##############################################################################
if __name__ == '__main__':
    # Set up logger
    root_logger = logging.getLogger()
    root_handler = logging.StreamHandler()
    rec_format = (
        "%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(funcName)s:"
        "%(message)s")
    formatter = logging.Formatter(rec_format, datefmt='%Y-%m-%d %H:%M:%S')
    root_handler.setFormatter(formatter)
    root_logger.addHandler(root_handler)
    # Attempt to hide geos callbacks; thanks Finn (2018)
    # https://stackoverflow.com/a/51529172
    logging.getLogger("shapely").setLevel("WARNING")

    # Add command-line argument handling to turn this into a tool.
    p = argparse.ArgumentParser(
        description="The residual grid mix Python tool.")
    p.add_argument(
        "-f", "--rec_file", default=None,
        help="path to NREL Green Power Data Excel workbook")
    p.add_argument(
        "-r", "--rec_method", default="zero",
        choices=["keep", "zero"],
        help="method for managing negative renewable generation")
    p.add_argument(
        "-a", "--agg_method", default="count",
        choices=['area', 'count'],
        help="method for aggregating REC generation from states to BA areas")
    p.add_argument(
        "-y", "--year", type=int, default=2020,
        help="generation year, defaults to 2020")
    p.add_argument(
        "-v", "--verbose", action='store_true',
        help="print results to console")
    p.add_argument(
        "-s", "--save", action="store_true",
        help="write results to CSV file")
    p.add_argument(
        "-l", "--log_level", default="INFO",
        choices=['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='set logging level, defaults to INFO')

    # Read arguments:
    args = p.parse_args()

    # Manage command-line arguments
    root_logger.setLevel(args.log_level)
    r_file = args.rec_file
    if r_file and not os.path.isfile(r_file):
        r_file = None
    mix_df = run(
        r_file,
        rec_handler=args.rec_method,
        agg_handler=args.agg_method,
        gen_yr=args.year,
        verbose=args.verbose,
        to_save=args.save
    )
