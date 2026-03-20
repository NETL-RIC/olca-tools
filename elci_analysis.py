#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# run.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os
import re
import json
import zipfile
import hashlib

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from netlolca.NetlOlca import NetlOlca


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module is designed for analyzing across ElectricityLCI
model variants for comparative analysis.

Last edited: 2026-03-20

Examples
--------
The following example analyzes four ElectricityLCI databases for differences.

First, define data directory where JSON-LD files are located
(I'm using the common 'data' dir in netlolca repo).

>>> data_dir = "data"

Then, define JSON-LD databases for analyzing.

1. Original baseline
2. New baseline
3. 2022 baseline w/ EBA.zip bulk trade
4. 2022 baseline w/ EIA API bulk trade

>>> js_var1 = os.path.join(
...     data_dir, "Federal_LCA_Commons-US_electricity_baseline.zip")
>>> js_var2 = os.path.join(
...     data_dir, "ELCI_1_jsonld_20240925_no-uncertainty.zip")
>>> js_var3 = os.path.join(
...     data_dir, "ELCI_2022_jsonld_20240926_eba.zip")
>>> js_var4 = os.path.join(
...     data_dir, "ELCI_2022_jsonld_20240925_eia.zip")

Create a data file manager; used to pass to methods.

>>> json_dict = {
...     '2016_flcac': js_var1,
...     '2016': js_var2,
...     '2022_eba': js_var3,
...     '2022_eia': js_var4,
... }

ANALYSIS 0 - Look for differences in JSON-LD files.

>>> results = run_comparison(js_var3, js_var4, True)

ANALYSIS 1 - U.S. Fuel Consumption Mixes.

>>> us_mix_df, baas = fuel_mix_analysis(json_dict)
>>> us_mix_df.round(5)
>>> plot_fuel_results(us_mix_df, 'Mix', units="fraction")

Optional - Check Balancing Authority Areas (BAs).
Extract the BA codes from each database.

>>> var1_bas = baas['2016_flcac']
>>> var2_bas = baas['2016']
>>> var3_bas = baas['2022_eba']
>>> var4_bas = baas['2022_eia']

Check to see if there differences in BAs amongst databases.

>>> set(var1_bas) - set(var2_bas)
>>> set(var2_bas) - set(var1_bas)
>>> set(var3_bas) - set(var4_bas)
>>> set(var4_bas) - set(var3_bas)

ANALYSIS 2 - Emissions Analysis.

Carbon Dioxide Emission Factors.

>>> co2_air_uuid = 'b6f010fb-a764-3063-af2d-bcb8309a97b7'
>>> us_co2_df = emission_analysis(json_dict, co2_air_uuid)
>>> us_co2_df.round(4)
>>> plot_fuel_results(us_co2_df, "CO2", "kg/MWh")

Particulate matter 2.5.

>>> pm25_uuid = '49a9c581-7c83-36b0-b1bd-455ea4c665a6'
>>> us_pm25_df = emission_analysis(json_dict, pm25_uuid)
>>> us_pm25_df.round(6)
>>> plot_fuel_results(us_pm25_df, "PM25", "kg/MWh")

Volatile organic compounds.

>>> voc_uuid = '6f861846-1c4c-3fc9-a198-56b2d9abd83b'
>>> us_voc_df = emission_analysis(json_dict, voc_uuid)
>>> us_voc_df.round(6)
>>> plot_fuel_results(us_voc_df, "VOC", "kg/MWh")

Lead, to air.

>>> lead_uuid = 'fe829136-3042-36e6-b4cb-7ff591e8db98'
>>> us_lead_df = emission_analysis(json_dict, lead_uuid)
>>> us_lead_df.round(9)
>>> plot_fuel_results(us_lead_df, 'Lead', 'kg/MWh')

Sulfur dioxide, to air.

>>> so2_uuid = 'f4973035-59f5-3bdc-b257-b274dcc04e0f'
>>> us_so2_df = emission_analysis(json_dict, so2_uuid)
>>> us_so2_df.round(4)
>>> plot_fuel_results(us_so2_df, "SO2", "kg/MWh")

Carbon monoxide, to air.

>>> co_uuid =  '187c525c-3715-388c-b303-a0671524a615'
>>> us_co_df = emission_analysis(json_dict, co_uuid)
>>> us_co_df.round(4)
>>> plot_fuel_results(us_co_df, "CO", "kg/MWh")

Nitrogen oxides, to air.

>>> nox_uuid = '4382ba18-dd21-3837-80b2-94283ef5490e'
>>> us_nox_df = emission_analysis(json_dict, nox_uuid)
>>> us_nox_df.round(4)
>>> plot_fuel_results(us_nox_df, "NOx", "kg/MWh")
"""


##############################################################################
# GLOBALS
##############################################################################
FUEL_CATS = [
    "ALL",
    "BIOMASS",
    "COAL",
    "GAS",
    "GEOTHERMAL",
    "HYDRO",
    "MIXED",
    "NUCLEAR",
    "OFSL",
    "OIL",
    "OTHF",
    "SOLAR",
    "SOLARTHERMAL",
    "WIND",
]
'''list : Electricity baseline primary fuel categories.'''


##############################################################################
# FUNCTIONS
##############################################################################
def compare_maps(map_a, map_b, label="V1 vs V2", verbose=False):
    """Run comparison of file hashes between two collections.

    Parameters
    ----------
    map_a : dict
        Collection of file names (keys) and their hashes (values)
    map_b : dict
        The same as ``map_a``, but a newer version for comparison.
    label : str, optional
        Display text, by default "V1 vs V2"
    verbose : bool, optional
        Whether to print results. Defaults to false.

    Notes
    -----
    The mapped dictionary hashes are based on :func:`get_json_fingerprint`.
    """
    all_files = set(map_a.keys()) | set(map_b.keys())
    results = {
        "unchanged": [],
        "modified": [],
        "added": [],
        "removed": []
    }
    for f in all_files:
        if f in map_a and f in map_b:
            if map_a[f] == map_b[f]:
                results["unchanged"].append(f)
            else:
                results["modified"].append(f)
        elif f in map_b:
            results["added"].append(f)
        else:
            results["removed"].append(f)

    if verbose:
        total = len(all_files)
        print(f"--- {label} ---")
        for category, files in results.items():
            print(f"{category.capitalize()}: {len(files)}")
        print(f"Total: {total}")

    return results


def get_emission_by_fuel(json_ld, e_uuid):
    """Return dictionary of emission amounts by primary fuel type.

    Parameters
    ----------
    json_ld : str
        A file path to a JSON-LD database.
    e_uuid : str
        A universally unique identifier to an emission.

    Returns
    -------
    dict
        A dictionary of primary fuel categories (keys) and their total
        emission amounts (i.e., scaled based on Balancing Authority mix
        percentages)

    Raises
    ------
    OSError
        Failed to find the JSON-LD file.
    ValueError
        Failed to find the emission flow in the JSON-LD database.
    """
    if not os.path.isfile(json_ld):
        raise OSError("Failed to find JSON-LD, %s" % json_ld)

    # Initialize fuel mix dictionary
    logging.info("Initializing fuel mix dictionary")
    fuel_dict = dict()
    for f_cat in FUEL_CATS:
        fuel_dict[f_cat] = 0.0

    # Initialize fuel name query
    q_fuel = re.compile("^from (\\w+) - (.*)$")

    logging.info("Reading JSON-LD file")
    netl = NetlOlca()
    netl.open(json_ld)
    netl.read()

    # Check for flow existence in JSON-LD
    flow = netl.query(netl.get_spec_class("Flow"), e_uuid)
    if flow:
        flow_str = "%s, %s" % (flow.name, flow.category)
        logging.info("Processing %s" % flow_str)
    else:
        raise ValueError("Failed to find emission flow!")

    # Find the U.S. grid consumption mix.
    q = re.compile("^Electricity; at grid; consumption mix - US - US$")
    r = netl.match_process_names(q)
    if len(r) == 1:
        logging.info("Found U.S. consumption mix process")
        us_uid = r[0][0]

        # US flows are by BA area.
        # Get ba mix values, then search provider for fuel-based inventory
        us_flows = netl.get_flows(us_uid, inputs=True, outputs=False)
        ba_mixes = us_flows['amount']
        ba_uuids = us_flows['provider']
        num_mixes = len(ba_mixes)
        logging.info("Processing %d BA areas" % num_mixes)
        for i in range(num_mixes):
            # This is the BA mix coefficient (at U.S. consumption level).
            ba_mix = ba_mixes[i]

            # Get input exchange values---these should be for primary fuels
            ba_uid = ba_uuids[i]
            ba_exchanges = netl.get_flows(ba_uid, inputs=True, outputs=False)
            ba_fuel_mixes = ba_exchanges['amount']
            ba_fuel_descr = ba_exchanges['description']
            ba_fuel_providers = ba_exchanges['provider']
            # Pull fuel names from description text.
            ba_fuel_names = []
            for ba_fuel in ba_fuel_descr:
                r = q_fuel.match(ba_fuel)
                f_name = ""
                if r:
                    f_name = r.group(1)
                ba_fuel_names.append(f_name)

            # For each primary fuel represented in a BA, get its provider:
            # these are the region-fuel LCIs
            num_fuels = len(ba_fuel_mixes)
            for j in range(num_fuels):
                fuel_name = ba_fuel_names[j]
                fuel_mix = ba_fuel_mixes[j]
                fuel_provider = ba_fuel_providers[j]

                # Dig into the provider's emissions
                fuel_emissions = netl.get_flows(
                    fuel_provider, inputs=False, outputs=True)

                # Match fuel emissions to the requested UUID;
                # NOTE: an emission may show up more than once in an exchange
                # table, so don't just return index!
                emis_uuids = fuel_emissions['uuid']
                num_uuids = len(emis_uuids)
                emis_index = [
                    k for k in range(num_uuids) if emis_uuids[k] == e_uuid]

                for e_idx in emis_index:
                    # Emissions are in units per MWh
                    # For example, 10% U.S. electricity from BA1 (ba_mix),
                    # which is powered 20% by coal (fuel_mix), which emits
                    # 100 kg/MWh of CO2 (e_val), then the U.S. value is
                    # .1 * .2 * 100.0 = 2 kg/MWh
                    e_val = fuel_emissions['amount'][e_idx]
                    fuel_dict[fuel_name] += ba_mix*fuel_mix*e_val

    logging.info("Done! Closing JSON-LD")
    netl.close()

    return fuel_dict


def get_fuel_mix(json_ld):
    """Return a tuple of electricity consumption mix by fuel type and a
    list of providers (e.g., balancing authority names).

    Parameters
    ----------
    json_ld : str
        JSON-LD file path, which includes the electricity baseline.

    Returns
    -------
    tuple
        Tuple of length two:

        - dict: Primary fuel categories and their consumption mix.
        - list: List of provider descriptions.

    Raises
    ------
    OSError
        If JSON-LD file is not found.
    """
    if not os.path.isfile(json_ld):
        raise OSError("Failed to find JSON-LD, %s" % json_ld)

    # Initialize fuel mix dictionary
    logging.info("Initializing fuel mix dictionary")
    fuel_dict = dict()
    for f_cat in FUEL_CATS:
        fuel_dict[f_cat] = 0.0

    ba_list = []

    # Initialize fuel name query
    q_fuel = re.compile("^from (\\w+) - (.*)$")

    logging.info("Reading JSON-LD file")
    netl = NetlOlca()
    netl.open(json_ld)
    netl.read()
    q = re.compile("^Electricity; at grid; consumption mix - US - US$")
    r = netl.match_process_names(q)
    if len(r) == 1:
        logging.info("Found U.S. consumption mix process")
        us_uid = r[0][0]

        # US flows are by BA area.
        # Get ba mix values, then search provider for fuel-based inventory
        us_flows = netl.get_flows(us_uid, inputs=True, outputs=False)
        ba_mixes = us_flows['amount']
        ba_uuids = us_flows['provider']
        ba_names = us_flows['description']
        num_mixes = len(ba_mixes)
        logging.info("Processing %d BA areas" % num_mixes)
        for i in range(num_mixes):
            # This is the BA mix coefficient.
            ba_mix = ba_mixes[i]
            ba_list.append(ba_names[i])

            # Get input exchange values---these should be for primary fuels
            ba_uid = ba_uuids[i]
            ba_exchanges = netl.get_flows(ba_uid, inputs=True, outputs=False)
            ba_fuel_mixes = ba_exchanges['amount']
            ba_fuel_descr = ba_exchanges['description']
            # Pull fuel names from description text.
            ba_fuel_names = []
            for ba_fuel in ba_fuel_descr:
                r = q_fuel.match(ba_fuel)
                f_name = ""
                if r:
                    f_name = r.group(1)
                ba_fuel_names.append(f_name)

            num_fuels = len(ba_fuel_mixes)
            for j in range(num_fuels):
                fuel_name = ba_fuel_names[j]
                fuel_mix = ba_fuel_mixes[j]

                # Here's the math:
                # Update the value of fuel_dict[fuel_name] with
                # `ba_mix` * `fuel_mix`. Because both coefficients
                # are fractions of one, we should be able to just
                # sum them up across all BA areas in the U.S.
                # for each fuel category
                fuel_dict[fuel_name] += ba_mix*fuel_mix

    logging.info("Done! Closing JSON-LD")
    netl.close()

    return (fuel_dict, ba_list)


def get_json_fingerprint(zip_path, byte_level=True):
    """Create byte hashes for each file in a zip.

    Parameters
    ----------
    zip_path : str
        File path to a zip (e.g., a JSON-LD).
    byte_level : bool, optional
        Whether to use byte-level hash; otherwise, the JSON files are loaded
        into memory, their keys sorted, and then hashed. By default true.

    Returns
    -------
    dict
        A dictionary of all member files in the zip and their SHA-256 hash.
    """
    fingerprints = {}
    with zipfile.ZipFile(zip_path, 'r') as z:
        for member in z.infolist():
            if member.filename.endswith('.json'):
                if byte_level:
                    # Read the raw bytes and hash them
                    content = z.read(member.filename)
                    digest = hashlib.sha256(content).hexdigest()
                else:
                    # Read semantic-level info
                    data = json.loads(z.read(member.filename))
                    normalized = json.dumps(
                        data, sort_keys=True).encode('utf-8')
                    digest = hashlib.sha256(normalized).hexdigest()
                # NOTE: member.filename includes subfolder path.
                fingerprints[member.filename] = digest
    return fingerprints


def emission_analysis(json_ld, e_uuid):
    """Run emission analysis on a JSON-LD database for a given flow UUID.

    Parameters
    ----------
    json_ld : dict
        A dictionary of JSON-LD database file paths.
    e_uuid : str
        A universally unique identifier for an emission flow.

    Returns
    -------
    pandas.DataFrame
        A dataframe of fuel-specific emission totals.
    """
    # Create empty data frame
    df = pd.DataFrame({'Fuel': FUEL_CATS})

    # Iterate over each JSON-LD file
    for k, v in json_ld.items():
        var_pm_fuel = get_emission_by_fuel(v, e_uuid)
        tmp_dict = {
            'Fuel': FUEL_CATS,
            k: [var_pm_fuel[x] for x in FUEL_CATS],
        }
        tmp_df = pd.DataFrame(tmp_dict)
        df = df.merge(
            tmp_df,
            how='left',
            on='Fuel'
        )
    # Add total row
    total_df = {'Fuel': ['TOTAL',]}
    for k,v in json_ld.items():
        total_df[k] = [df[k].sum(),]
    df = pd.concat([df, pd.DataFrame(total_df)])

    return df


def fuel_mix_analysis(json_ld, add_total=False):
    """Run the U.S. fuel mix analysis.

    Parameters
    ----------
    json_ld : dict
        A dictionary of JSON-LD file paths.
    add_total : bool, optional
        If true, the total column is the returned data frame, by default False

    Returns
    -------
    tuple
        A tuple of length two: pandas.DataFrame of results and dictionary of
        Balancing Authority codes for each database processed.
    """
    # Initialize data frame
    df = pd.DataFrame({'Fuel': FUEL_CATS})

    ba_dict = {}

    for k,v in json_ld.items():
        var_us_mix, var_bas = get_fuel_mix(v)

        logging.info("Cleaning BA names")
        var_bas = [x.replace("eGRID 2016. From ", "") for x in var_bas]
        var_bas = [x.replace("eGRID 2021. From ", "") for x in var_bas]
        var_bas = [x.replace("eGRID 2023. From ", "") for x in var_bas]
        var_bas = sorted(var_bas)

        # Append BA list to return dictionary
        ba_dict[k] = var_bas

        # Append fuel mix to data frame
        tmp_dict = {
            'Fuel': FUEL_CATS,
            k: [var_us_mix[x] for x in FUEL_CATS],
        }
        tmp_df = pd.DataFrame(tmp_dict)
        df = df.merge(
            tmp_df,
            how='left',
            on='Fuel'
        )

    # Add total row
    if add_total:
        total_df = {'Fuel': ['TOTAL',]}
        for k,v in json_ld.items():
            total_df[k] = [df[k].sum(),]
        df = pd.concat([df, pd.DataFrame(total_df)])

    return (df, ba_dict)


def generation_process_checker(js_dict):
    """Check if each generation process is referenced in a generation mix
    within each JSON-LD.

    This can be helpful to identify generation processes created, but
    never used in a mix.

    Parameters
    ----------
    js_dict : dict
        A dictionary of JSON-LD file paths (keys).
    """
    # NOTE: This is slow (about 30 minutes per JSON-LD).
    for label, json_ld in js_dict.items():
        logging.info("Reading JSON-LD file for '%s'" % label)
        netl = NetlOlca()
        netl.open(json_ld)
        netl.read()
        # Find all generation mix processes
        q1 = re.compile("^Electricity; at grid; generation mix - (.*)$")
        r1 = netl.match_process_names(q1)
        num_mixes = len(r1)
        logging.info("%s: Processing %d BA areas" % (label, num_mixes))
        for r in r1:
            ba_uid, ba_name = r
            # Get input exchange values---these should be for primary fuels
            ba_flows = netl.get_flows(
                ba_uid, inputs=True, outputs=False
            )

            # Find all fuel gen processes for this BA. *slow*
            q2 = re.compile("^Electricity - (.*) - %s$" % ba_name)
            r2 = netl.match_process_names(q2)

            # Report total fuel gen processes out of exchange count.
            logging.info(
                "%s: Found %d/%d fuels for %s" % (
                    label,
                    len(r2),
                    len(ba_flows['description']),
                    ba_name
                )
            )
            for my_tuple in r2:
                _, fuel_name = my_tuple
                found_fuel = False
                for ex_description in ba_flows['description']:
                    if fuel_name in ex_description:
                        found_fuel = True
                if not found_fuel:
                    logging.warning(
                        "%s: !!! Failed to find %s exchange in %s" % (
                            label, fuel_name, ba_name
                        )
                    )
        logging.info("Done! Closing JSON-LD")
        netl.close()


def missing_input_checker(json_ld):
    """List processes without input exchanges.

    Parameters
    ----------
    json_ld : str
        File path to a JSON-LD file from ElectricityLCI.

    Returns
    -------
    list
        A list of process UUID and process name tuples that have no
        input exchanges. This may be helpful identifying system processes
        or unit processes that need further examining.
    """
    # Return list of tuples of processes without any input exchanges.
    netl = NetlOlca()
    netl.open(json_ld)
    netl.read()

    p_list = []

    # Read through processes
    for pid in netl.get_spec_ids(netl.get_spec_class("Process")):
        p = netl.query(netl.get_spec_class("Process"), pid)
        has_inputs = False
        for ex in p.exchanges:
            if ex.is_input:
                has_inputs = True
        if not has_inputs:
            my_tuple = (pid, p.name)
            p_list.append(my_tuple)

    netl.close()
    logging.info("Found %d processes without input exchanges" % len(p_list))
    return p_list


def plot_fuel_results(df, y_cat, units="", to_save=True):
    """A helper method for plotting results and saving to image file.

    Parameters
    ----------
    df : pandas.DataFrame
        A results data frame (i.e., either from :func:`emission_analysis` or
        :func:`fuel_mix_analysis`).
    y_cat : str
        A string of whatever the result amounts represent.
    units : str, optional
        The units for the result amounts, by default ""
    to_save : bool, optional
        Whether to save the figure to PNG file, by default True
    """
    # Creates a three column data frame of 'Fuel' categories, 'model'
    # categories (i.e., the column headers in `df`), and y_cat (i.e.,
    # whatever the numbers represent, such as CO2 emissions).
    # Source: Trenton McKinney (https://stackoverflow.com/a/38808042)
    dfm = pd.melt(
        df,
        id_vars="Fuel",
        var_name="Model",
        value_name=y_cat
    )
    g = sns.catplot(
        x='Fuel',
        y=y_cat,
        hue='Model',
        data=dfm,
        kind='bar',
        height=5,
        aspect=4,
    )

    if units:
        y_label = "%s (%s)" % (y_cat, units)
        g.set(ylabel=y_label)

    # Add a second legend; crop out the first
    ncols = len(dfm['Model'].unique())
    g.figure.legend(loc=9, ncol=ncols, frameon=False, title="Model")
    if to_save:
        out_fig = "%s.png" % y_cat.lower()
        g.figure.savefig(out_fig)
    plt.show()


def run_comparison(js1, js2, byte_level):
    """Quantify modifications, additions and removals between two JSON-LDs.

    Parameters
    ----------
    js1 : str
        File path to a JSON-LD.
    js2 : str
        File path to a second JSON-LD (e.g., a newer version of js1).
    byte_level : bool
        Whether to compare on byte-level (true); otherwise, will read the
        JSON data, sort the keys, then create the byte map (use this if you
        find that every file has changed even though you think they have not).

    Notes
    -----
    If checking against two JSON-LDs from ElectricityLCI, you will likely find
    a consistent difference in the number of product systems, as they are
    created with a random hash every run. These will show up as "added" and
    "removed" in the results.
    """
    js1_file = os.path.basename(js1)
    js2_file = os.path.basename(js2)
    label = "%s vs %s" % (js1_file, js2_file)
    v1 = get_json_fingerprint(js1, byte_level)
    v2 = get_json_fingerprint(js2, byte_level)
    results = compare_maps(v1, v2, label)

    # Keys include 'unchanged', 'modified', 'added', and 'removed'.
    print(f"---- {label} ----")
    for category, files in results.items():
        results[category] = sorted(files)
        compartments = {}
        for f in files:
            compartment = os.path.dirname(f)
            if compartment in compartments:
                compartments[compartment].append(f)
            else:
                compartments[compartment] = [f,]
        print(category)
        for k, v in compartments.items():
            if k == "":
                k = "root"
            print(f"  {k}: {len(v)}")
        print(f"  total: {len(files)}")

    return results


##############################################################################
# MAIN
##############################################################################
if __name__ == '__main__':
    import sys

    # Set up logger
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    rec_format = (
        "%(asctime)s, %(name)s.%(funcName)s: "
        "%(message)s")
    formatter = logging.Formatter(rec_format, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel("INFO")

    # Optional for Jupyter notebooks to get rid of the pink
    logger.handlers[0].stream = sys.stdout

    # See 'Examples' in the module documentation above.
