#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# primary_fuel_finder.py

__doc__ = """
The goal is to find the primary fuel categories associated with electricity
generating facilities over a set of years to see how their primary fuel
categories change over time.

A basic outline:

1.  Create a modelconfig in ElectricityLCI and lower its
    `min_plant_percent_generation_from_primary_fuel_category` value to 50;
    set `keep_mixed_plant_category` to false.
2.  Define a set of years (e.g., 2000-2022).
3.  For each year, call the :func:`eia923_primary_fuel` method (in
    eia923_generation.py in ElectricityLCI) using the 'net generation'
    method (alternatively re-run this analysis using the 'total fuel
    consumption' method)
4.  Fill a master data frame with the results from each year (row names are
    the facility IDs---new facilities come online over time, so the list
    of all facilities grows with time---columns are the years and the values
    are the primary fuel categories---use blank or empty string to indicate
    years without information or mixed category).

A secondary goal is to identify facilities that change from one specified fuel
to another (e.g., coal to gas).

Requirements:

    - Pandas
    - Numpy
    - ElectricityLCI (https://github.com/NETL-RIC/ElectricityLCI)

Authors:

    - Tyler W. Davis
    - Daniel Naiman

Last updated:
    2025-09-02
"""

# Initial imports
import pandas as pd
import numpy as np

import electricitylci.model_config as config
from electricitylci.utils import get_logger

# Create a stream logger
log = get_logger(stream=True, rfh=False)

# Create a temporary model configuration based on ELCI 2022 YAML; update
# two of the configuration parameters for this test.
name = "temp"
specs = config._load_model_specs('ELCI_2022')
specs['min_plant_percent_generation_from_primary_fuel_category'] = 50
specs['keep_mixed_plant_category'] = False
config.check_model_specs(specs)
config.model_specs = config.ModelSpecs(specs,  name)

# Now, import the other eLCI modules
# NOTE: upon import, they will get a copy of the model config, which needs
# to be defined first!
import electricitylci.eia923_generation as eia_923
import electricitylci.eia860_facilities as eia_860


# FUNCTIONS
def create_boiler_data_frame(year):
    """Create a data frame with boiler-level fuel consumption and primary fuel data.

    Parameters
    ----------
    year : int
        The year of interest for boiler fuel consumption and primary fuel.

    Returns
    -------
    pandas.DataFrame
        A data frame with the following columns:
        - plant_id (int)
        - boiler_id (str)
        - total_fuel_consumption_mmbtu (float)
        - FuelCategory (str)
        - PrimaryFuel (str)
        - primary_fuel_percent (float)
        - YEAR (int)
    """
    # Get boiler fuel consumption data
    boiler_fuel = eia_923.eia923_boiler_fuel(year)

    # Get boiler design information
    boiler_design = eia_860.eia860_boiler_info_design(year)

    # Calculate monthly fuel consumption for each boiler
    fuel_heating_value_monthly = [
        "mmbtu_per_unit_january", "mmbtu_per_unit_february", "mmbtu_per_unit_march",
        "mmbtu_per_unit_april", "mmbtu_per_unit_may", "mmbtu_per_unit_june",
        "mmbtu_per_unit_july", "mmbtu_per_unit_august", "mmbtu_per_unit_september",
        "mmbtu_per_unit_october", "mmbtu_per_unit_november", "mmbtu_per_unit_december"
    ]
    fuel_quantity_monthly = [
        "quantity_of_fuel_consumed_january", "quantity_of_fuel_consumed_february",
        "quantity_of_fuel_consumed_march", "quantity_of_fuel_consumed_april",
        "quantity_of_fuel_consumed_may", "quantity_of_fuel_consumed_june",
        "quantity_of_fuel_consumed_july", "quantity_of_fuel_consumed_august",
        "quantity_of_fuel_consumed_september", "quantity_of_fuel_consumed_october",
        "quantity_of_fuel_consumed_november", "quantity_of_fuel_consumed_december"
    ]

    # Calculate total fuel consumption for each boiler
    boiler_fuel["total_fuel_consumption_mmbtu"] = (
        np.multiply(
            boiler_fuel[fuel_heating_value_monthly],
            np.asarray(boiler_fuel[fuel_quantity_monthly])
        )
    ).sum(axis=1, skipna=True)

    # Merge with boiler design information
    boiler_data = boiler_fuel.merge(
        boiler_design[["plant_id", "boiler_id", "firing_type_1"]],
        on=["plant_id", "boiler_id"],
        how="left"
    )

    # Determine primary fuel for each boiler
    boiler_data = determine_boiler_primary_fuel(boiler_data)

    # Add year column
    boiler_data['YEAR'] = year

    # Filter for positive fuel consumption
    positive_mask = boiler_data["total_fuel_consumption_mmbtu"] > 0
    log.info(f"Filter to {positive_mask.sum()} boilers - from positive fuel consumption")
    boiler_data = boiler_data.loc[positive_mask, :]

    # Filter for threshold primary fuel consumption
    threshold_mask = boiler_data["primary_fuel_percent"] >= config.model_specs.min_plant_percent_generation_from_primary_fuel_category/100
    log.info(f"Filter to {threshold_mask.sum()} boilers - from primary fuel threshold")
    boiler_data = boiler_data.loc[threshold_mask, :]

    # Ensure proper data types
    boiler_data['plant_id'] = boiler_data['plant_id'].astype('int')
    boiler_data['YEAR'] = boiler_data['YEAR'].astype('int')

    return boiler_data


def create_data_frame(year, method):
    """Abstraction of parts of eLCI's :func:`build_generation_data`

    Parameters
    ----------
    year : int
        The year of interest for facility generation and primary fuel.
    method : str
        The method for determining the primary fuel category.
        It may be one of two options:

        - "Net Generation (Megawatthours)"
        - "Total Fuel Consumption MMBtu"

    Returns
    -------
    pandas.DataFrame
        A data frame with the following columns:

        - Plant Id (int)
        - State (str)
        - Total Fuel Consumption MMBtu (int)
        - Net Generation (Megawatthours) (float)
        - efficiency (float)
        - Plant Name (str)
        - YEAR (int)
        - FuelCategory (str)
        - PrimaryFuel (str)
        - primary fuel percent gen (float)
    """
    # Get the generation and its efficiency and primary fuel category
    gen = eia_923.eia923_download_extract(year)
    eff = eia_923.calculate_plant_efficiency(gen)
    pfc = eia_923.eia923_primary_fuel(year=year, method_col=method)

    # Combine these three datasets together
    df = eff.merge(
        gen[['Plant Id', 'State', 'Plant Name', 'YEAR']],
        on=['Plant Id', 'State'],
        how='left'
    ).drop_duplicates()
    df = df.merge(
        pfc,
        on='Plant Id',
        how='left'
    )

    # Filter for positive generation, reasonable efficiency, and threshold
    # primary fuel generation
    pg_mask = df["Net Generation (Megawatthours)"] >= 0
    log.info(
        "Filter to %d facilities - from negative generation" % pg_mask.sum()
    )
    df = df.loc[pg_mask, :]

    df = eia_923.efficiency_filter(
        df,
        config.model_specs.egrid_facility_efficiency_filters
    )
    log.info("Filter to %d facilities - from efficiency" % len(df))

    mp_mask = df["primary fuel percent gen"] >= config.model_specs.min_plant_percent_generation_from_primary_fuel_category
    log.info(
        "Filter to %d facilities - from primary fuel generation" % mp_mask.sum()
    )
    df = df.loc[mp_mask, :]
    df['Plant Id'] = df['Plant Id'].astype('int')
    df['YEAR'] = df['YEAR'].astype('int')

    return df


def determine_boiler_primary_fuel(boiler_data):
    """Determine the primary fuel for each boiler based on fuel consumption.

    Parameters
    ----------
    boiler_data : pandas.DataFrame
        DataFrame containing boiler fuel consumption data

    Returns
    -------
    pandas.DataFrame
        DataFrame with added PrimaryFuel and primary_fuel_percent columns
    """
    # Define fuel categories mapping (similar to FUELCAT_MAP in ampd_plant_emissions.py)
    fuel_categories = {
        "AB": "BIOMASS", "BIT": "COAL", "DFO": "OIL", "GEO": "GEOTHERMAL",
        "LIG": "COAL", "NG": "GAS", "NUC": "NUCLEAR", "OBG": "BIOMASS",
        "OBL": "BIOMASS", "OBS": "BIOMASS", "RC": "COAL", "RFO": "OIL",
        "SUB": "COAL", "SUN": "SOLAR", "WAT": "HYDRO", "WC": "COAL",
        "WDL": "BIOMASS", "WDS": "BIOMASS", "WND": "WIND", "WO": "OIL"
    }

    # First, aggregate fuel consumption by boiler_id to handle multiple fuel types per boiler
    # This ensures we have exactly one row per boiler
    boiler_agg = boiler_data.groupby(['plant_id', 'boiler_id', 'plant_name', 'operator_name', 'firing_type_1'], as_index=False).agg({
        'total_fuel_consumption_mmbtu': 'sum',
        'total_fuel_consumption_quantity': 'sum'
    })

    # Now determine primary fuel based on fuel consumption by fuel type
    # Group by boiler and fuel type to get total consumption per fuel type per boiler
    fuel_by_boiler = boiler_data.groupby(['plant_id', 'boiler_id', 'plant_name', 'operator_name', 'firing_type_1', 'reported_fuel_type_code'], as_index=False).agg({
        'total_fuel_consumption_mmbtu': 'sum',
        'total_fuel_consumption_quantity': 'sum'
    })

    # For each boiler, find the fuel type with highest consumption
    primary_fuel_data = []

    for _, boiler_row in boiler_agg.iterrows():
        plant_id = boiler_row['plant_id']
        boiler_id = boiler_row['boiler_id']

        # Get all fuel consumption data for this specific boiler
        boiler_fuel_data = fuel_by_boiler[
            (fuel_by_boiler['plant_id'] == plant_id) &
            (fuel_by_boiler['boiler_id'] == boiler_id)
        ]

        if len(boiler_fuel_data) > 0:
            # Find the fuel type with highest consumption
            max_fuel_row = boiler_fuel_data.loc[boiler_fuel_data['total_fuel_consumption_mmbtu'].idxmax()]
            primary_fuel_code = max_fuel_row['reported_fuel_type_code']
            primary_fuel_category = fuel_categories.get(primary_fuel_code, "Unknown")

            # Calculate percentage
            total_consumption = boiler_fuel_data['total_fuel_consumption_mmbtu'].sum()
            primary_fuel_consumption = max_fuel_row['total_fuel_consumption_mmbtu']
            primary_fuel_percent = primary_fuel_consumption / total_consumption if total_consumption > 0 else 0

            primary_fuel_data.append({
                'plant_id': plant_id,
                'boiler_id': boiler_id,
                'plant_name': boiler_row['plant_name'],
                'operator_name': boiler_row['operator_name'],
                'firing_type_1': boiler_row['firing_type_1'],
                'total_fuel_consumption_mmbtu': boiler_row['total_fuel_consumption_mmbtu'],
                'total_fuel_consumption_quantity': boiler_row['total_fuel_consumption_quantity'],
                'PrimaryFuel': primary_fuel_category,
                'primary_fuel_percent': primary_fuel_percent,
                'FuelCategory': primary_fuel_category
            })
        else:
            # Fallback if no fuel data found
            primary_fuel_data.append({
                'plant_id': plant_id,
                'boiler_id': boiler_id,
                'plant_name': boiler_row['plant_name'],
                'operator_name': boiler_row['operator_name'],
                'firing_type_1': boiler_row['firing_type_1'],
                'total_fuel_consumption_mmbtu': boiler_row['total_fuel_consumption_mmbtu'],
                'total_fuel_consumption_quantity': boiler_row['total_fuel_consumption_quantity'],
                'PrimaryFuel': "Unknown",
                'primary_fuel_percent': 1.0,
                'FuelCategory': "Unknown"
            })

    return pd.DataFrame(primary_fuel_data)


def find_boiler_category_switches(df, from_category, to_category):
    """
    Find boiler IDs that have switched from 'from_category' to 'to_category'.

    Args:
        df (pd.DataFrame): The pivoted DataFrame with boiler IDs as index and years as columns.
        from_category (str): The initial fuel category to switch from.
        to_category (str): The target fuel category to switch to.

    Returns:
        pd.DataFrame: A DataFrame containing only the rows (boiler IDs) that made the switch, along with the year of the switch.
    """
    switching_boilers = []

    # Iterate over each row (boiler ID)
    for boiler_id, row in df.iterrows():
        # Drop NaN values and convert to a list of categories in chronological order
        categories_over_time = row.dropna().tolist()
        years_in_data = row.dropna().index.tolist()

        if len(categories_over_time) < 2:
            continue # Need at least two data points to show a switch

        # Check for switches
        for i in range(len(categories_over_time) - 1):
            current_category = categories_over_time[i]
            next_category = categories_over_time[i+1]
            current_year = years_in_data[i]
            next_year = years_in_data[i+1]

            # Consider unique adjacent categories for a switch, ignoring multiple same entries
            if current_category == from_category and next_category == to_category and current_category != next_category:
                switching_boilers.append({
                    'boiler_id': boiler_id,
                    'From_Category': from_category,
                    'To_Category': to_category,
                    'Switch_Year_Start': current_year,
                    'Switch_Year_End': next_year
                })
                break # Only need to find one instance of the switch per boiler

    if switching_boilers:
        return df.loc[[item['boiler_id'] for item in switching_boilers]]
    else:
        return pd.DataFrame() # Return empty DataFrame if no switches found


def find_category_switches(df, from_category, to_category):
    """
    Find Plant Ids that have switched from 'from_category' to 'to_category'.

    Args:
        df (pd.DataFrame): The pivoted DataFrame with Plant Ids as index and years as columns.
        from_category (str): The initial fuel category to switch from.
        to_category (str): The target fuel category to switch to.

    Returns:
        pd.DataFrame: A DataFrame containing only the rows (Plant Ids) that made the switch, along with the year of the switch.
    """
    switching_plants = []

    # Iterate over each row (Plant Id)
    for plant_id, row in df.iterrows():
        # Drop NaN values and convert to a list of categories in chronological order
        categories_over_time = row.dropna().tolist()
        years_in_data = row.dropna().index.tolist()

        if len(categories_over_time) < 2:
            continue # Need at least two data points to show a switch

        # Check for switches
        for i in range(len(categories_over_time) - 1):
            current_category = categories_over_time[i]
            next_category = categories_over_time[i+1]
            current_year = years_in_data[i]
            next_year = years_in_data[i+1]

            # Consider unique adjacent categories for a switch, ignoring multiple same entries
            if current_category == from_category and next_category == to_category and current_category != next_category:
                switching_plants.append({
                    'Plant Id': plant_id,
                    'From_Category': from_category,
                    'To_Category': to_category,
                    'Switch_Year_Start': current_year,
                    'Switch_Year_End': next_year
                })
                break # Only need to find one instance of the switch per plant

    if switching_plants:
        return df.loc[[item['Plant Id'] for item in switching_plants]]
    else:
        return pd.DataFrame() # Return empty DataFrame if no switches found


def run_boilers():
    years = [x for x in range(2011, 2023)]

    df = None
    for year in years:
        log.info(f"Processing year {year}")
        try:
            temp = create_boiler_data_frame(year)
            if df is None and temp is not None:
                df = temp.copy()
            elif df is not None and temp is not None:
                df = pd.concat([df, temp], ignore_index=True)
        except Exception as e:
            log.error(f"Error processing year {year}: {e}")
            continue

    if df is None:
        log.error("No data was processed successfully")
        exit(1)

    log.info(f"Total boiler-year records: {len(df)}")

    df.to_csv("boiler_fuel_data_full.csv", index=False)

    # Add a new column that is the combination of the boiler_id and plant_id
    # A lot of boiler IDs are not unique, so we need to use the plant_id to make them unique
    df['full_boiler_id'] = df['boiler_id'] + '_' + df['plant_id'].astype(str)
    pivot_df = df.pivot(index='full_boiler_id', columns='YEAR', values='FuelCategory')
    pivot_df.to_csv("boiler_pfc.csv")

    # Filter for only specific fuel categories
    target_categories = ['GAS', 'COAL']
    mask = pivot_df.isin(target_categories).any(axis=1)
    pivot_df.loc[mask, :].to_csv("boiler_pfc-ng-coal.csv")

    # Find boilers that switched from COAL to GAS
    swap_df = find_boiler_category_switches(pivot_df, 'COAL', 'GAS')
    if not swap_df.empty:
        swap_df.to_csv("boiler_coal-to-ng.csv")
        log.info(f"Found {len(swap_df)} boilers that switched from COAL to GAS")
    else:
        log.info("No boilers found that switched from COAL to GAS")

    log.info("Analysis complete. Check the output CSV files for results.")


def run_plants():
    # Define the years of interest; EIA923 appears to go back to 2008
    # For historical data (1970 onwards), see
    #   https://www.eia.gov/electricity/data/eia923/eia906u.php
    #   Looks like most of what is needed is here, but needs pre-processing.
    # Challenges with Form EIA 923
    # - 2008 does not appear to have the right worksheets
    # - 2009 issues:
    #   Wrong Excel header row count (should be 7 not 5)
    #   The CSV file does not match the "{YEAR}_Final" requirement in search
    #   'Plant Id' is 'Plant ID'
    #   'YEAR' is 'Year'
    #   'Total Fuel Consumption MMBtu' is 'TOTAL FUEL CONSUMPTION MMBTUS'
    #   'Net Generation (Megawatthours)' is 'NET GENERATION (megawatthours)'
    # - 2010 is the same as 2009
    # - 2011 works!
    years = [x for x in range(2011, 2023)]

    # Define the method for primary fuel categorization
    method = "Net Generation (Megawatthours)"

    df = None
    for year in years:
        temp = create_data_frame(year, method)
        if df is None and temp is not None:
            df = temp.copy()
        elif df is not None and temp is not None:
            df = pd.concat([df, temp], ignore_index=True)
    # Not sure why there were duplicates in my first run; maybe memory issue?
    df = df.drop_duplicates()

    # Create the pivot table
    pivot_df = df.pivot(index='Plant Id', columns='YEAR', values='FuelCategory')
    pivot_df.to_csv("pfc.csv")

    # Filter for only specific fuel categories
    target_categories = ['GAS', 'COAL']
    mask = pivot_df.isin(target_categories).any(axis=1)
    pivot_df.loc[mask, :].to_csv("pfc-ng-coal.csv")

    # Find plants that switched from COAL to GAS
    swap_df = find_category_switches(pivot_df, 'COAL', 'GAS')
    swap_df.to_csv("coal-to-ng-plants.csv")


if __name__ == '__main__':
    run_boilers()
    # run_plants()
