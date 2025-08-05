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

"""

# Initial imports
import pandas as pd

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


# FUNCTIONS
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


if __name__ == '__main__':
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
