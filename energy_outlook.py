#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# energy_outlook.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os
import re

import pandas as pd

from electricitylci.utils import read_ba_codes
from netlolca.NetlOlca import NetlOlca


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module is a part of a project funded by the United States
Department of Energy National Energy Technology Laboratory.

This module provides a run method designed to connect to an openLCA project
(either directly via IPC-Server or indirectly via JSON-LD) and create outlook
grid mix processes for electricity generation at Balancing Authority areas.

This run method assumes that the CSV file provided is in the same format
as those generated for AEO forecasts. For each balancing authority, the
run method replaces the grid mixes with the outlook grid mixes.

If a new fuel source is added to a balancing authority, this results in
a new flow being generated rather than modifying an existing flow. If a
flow in the JSON-LD file is not found in the AEO forecast, that flow is
set to zero, but still included.

If a balancing authority name is not found in ba_codes.csv, the run method
will not replace any of the flow values. To ensure this does not occur,
check that all balancing authority names are included in ba_codes.csv.
If a name is missing, add it to the csv along with its code.

The run method takes several arguments that can either be run directly via a
method call or via the command line using the CLI parameters:

    -c {1,2}, --connection {1,2}
        1: IPC-Server 2: JSON-LD
    -d directory, --directory
        folder path containing AEO mix CSV files
    -f file_name, --file_name
        AEO mix file name
    -p P_FILE, --p_file P_FILE
        JSON-LD file (optional)

Examples
--------
From within Python:

>>> run(con=2, json_file="JSON_FILE_NAME_HERE.zip", csv_dir="data", csv_name="CSV_NAME_HERE.csv")

From the command line:

$ python residual_grid_mix.py -c 2 -d data -f CSV_NAME_HERE.csv -p JSON_FILE_NAME_HERE.zip

Notes
-----
This module is based on the residual grid mix run.py, also provided in
the tools directory of this repository.

The input datasets used by this module are produced by the AEO class, developed
as a part of the "scenario modeler" project under the Electricity Baseline
maintenance.

Warning!

Multiple runs of the run() method on the same openLCA project will result
in multiple instances of "Electricity; at grid; outlook generation mix"
processes, whether or not they represent different outlook mix methods.

If you need/want to update existing outlook mix processes, you should
delete old ones first!

The 2016 electricity baseline is organized into several tiers. Those of
interest are:

-   'Electricity; at user; consumption mix - ' US - US / x - FERC / y - BA
    (converts flow from 2300V to 120 V; includes loss value; not updated)

    -   'Electricity; at grid; consumption mix - ' US - US / x - FERC /
        y - BA (account for electricity trading; copied as outlook mix)

        -   'Electricity; at grid; generation mix - xxx - BA'
            (based on primary fuels; updated to outlook mix)

            -   'Electricity - FUEL CATEGORY - xxx'
                (BA inventory from primary fuel generation; no change)

Version:
    2.1.0
Last Edited:
    2025-07-29
"""
__all__ = [
    'FUEL_COL_NAME',
    'GEN_COL_NAME',
    'REG_COL_NAME',
    'USED_COL_NAME',
    'convert_primary_fuel',
    'get_fuel_dict',
    'get_new_process',
    'get_outlook_mix',
    'get_outlook_mix_description',
    'make_ba_dict',
    'make_forecast_process_name',
    'make_outlook_gen',
    'run',
    'update_exchange_to_outlook',
    'update_providers',
]


##############################################################################
# GLOBALS
##############################################################################
REG_COL_NAME = "Subregion"
'''str : Pandas data column for outlook mix region name.'''
FUEL_COL_NAME = "FuelCategory"
'''str : Pandas data column for outlook mix fuel name.'''
GEN_COL_NAME = "Gen_Ratio_new"
'''str : Pandas data column for outlook mix generation ratio name.'''
USED_COL_NAME = "Used"
'''str : Pandas data column name for tracking if a row has been used.'''


##############################################################################
# FUNCTIONS
##############################################################################
def convert_primary_fuel(name):
    """Helper function that takes a primary fuel name and converts it to its
    primary fuel code.

    Parameters
    ----------
    name : str
        A fuel name

    Returns
    -------
    str
        Primary fuel code (or empty string if not found)
    """
    r_val = ""
    if isinstance(name, str):
        name = name.lower()
        d = get_fuel_dict()
        r_val = d.get(name, "")

    return r_val


def find_new_process_id(n, name):
    """Query the openLCA database for a process with a given name.

    Parameters
    ----------
    n : NetlOlca
        NetlOlca class instance connected to an openLCA database or JSON-LD.
    name : str
        Process name

    Returns
    -------
    str, NoneType
        The UUID for the matched process name (or None if not found).
    """
    # Helper function to get the new process UUID after a database update.
    q = re.compile(name)
    matches = n.match_process_names(q)
    num_matches = len(matches)
    if num_matches == 1:
        logging.info("Found new process, '%s'" % name)
        return matches[0][0]
    elif num_matches == 0:
        logging.error("Failed to find new process, '%s'!" % name)
        return None
    elif num_matches > 1:
        logging.warning(
            "Found %d matches for new process, '%s'!" % (num_matches, name))
        return None


def get_fuel_dict():
    """Dictionary of textual fuel technologies (key) to their fuel names
    as found in the electricityLCI inventory data.

    Returns:
        (dict):
            Dictionary of fuel technology configurable names to their
            electricityLCI names, including renewables but excluding
            advanced technologies.

    Note:
        Source:
            NETL, "Scenario modeler."
            https://github.com/KeyLogicLCA/scenario-modeler/blob/274909b307a5490381105d8d53bb587faf7ea5af/grid_mixer/elci_utils.py#L2453
    """
    fuel_dict = {
        "bio": 'BIOMASS',
        "biomass": 'BIOMASS',
        "coal": 'COAL',
        "gas": 'GAS',
        "natural gas": 'GAS',
        "geo": 'GEOTHERMAL',
        "geothermal": 'GEOTHERMAL',
        "hydro": 'HYDRO',
        "hydroelectric": 'HYDRO',
        "hydro-electric": 'HYDRO',
        "mixed": 'MIXED',
        "nuclear": 'NUCLEAR',
        "other fossil": 'OFSL',
        "oil": 'OIL',
        "petro": 'OIL',
        "petroleum": 'OIL',
        "other": 'OTHF',
        "other fuels": 'OTHF',
        "solar": 'SOLAR',
        "solar thermal": 'SOLARTHERMAL',
        "wind": 'WIND'
    }
    return fuel_dict


def get_new_process(n, pid, d_txt="", at_grid=True, is_gen=True):
    """Create a new openLCA process object based on a given process.

    Parameters
    ----------
    n : NetlOlca
        Instance of NetlOlca class connected to an openLCA project.
    pid : str
        An existing universally unique identifier (UUID) for a process.
    d_txt : str, optional
        Description text for the new process, by default ""
    at_grid : bool, optional
        Whether process is "at grid"; otherwise, "at user"; defaults to true.
    is_gen : bool, optional
        Whether process is "generation"; otherwise, "consumption"; by default
        true.

    Returns
    -------
    olca_schema.Process
        A new process class.
    """
    # Search for process and create a dictionary w/ its meta data
    p = n.query(n.get_spec_class("Process"), pid)
    p_dict = {}
    if p is None:
        logging.warning("Failed to find process '%s'" % pid)
    else:
        p_dict = p.to_dict()

        # Reset UUID and last edited date (updated by olca-schema class)
        p_dict['@id'] = None
        p_dict['lastChange'] = None
        p_dict['name'] = make_forecast_process_name(p.name, at_grid, is_gen)
        p_dict['processDocumentation']['validFrom'] = '2020-01-01'
        p_dict['processDocumentation']['validUntil'] = '2020-12-31'

    # Update existing description or create new
    if isinstance(p_dict['description'], str):
        p_dict['description'] += " "
        p_dict['description'] += d_txt
    else:
        p_dict['description'] = d_txt

    return n.get_spec_class("Process").from_dict(p_dict)


def get_outlook_mix(csv_dir, csv_name):
    """Read AEO mix data file.

    Parameters
    ----------
    csv_dir : str
        Data folder where AEO grid mix CSV files are located.
    csv_name : str
        Name of the desired CSV file.

    Returns
    -------
    pandas.DataFrame
        AEO grid mix data at the Balancing Authority level.

    Raises
    ------
    OSError
        For missing data file (check data folder path)

    Notes
    -----
    The AEO CSV file should be generated using scenario modeler's AEO class.
    Copy the CSV file to the data folder and enter the file name
    in order to run the tool.
    The CSV file is organized by columns, such that the first column, 'Fuel',
    represents the primary fuel categories (e.g., 'Biomass,' 'Coal,' and
    'Wind'), and each subsequent column is a Balancing Authority (named by
    its code) with values representing the decimal fraction (i.e., values
    should add to one). Balancing authorities with fuel fractions that add
    to zero are undefined (i.e., can be ignored).
    """
    # Create CSV path and read CSV
    csv_path = os.path.join(csv_dir, csv_name)
    if not os.path.isfile(csv_path):
        raise OSError("Missing file, %s" % csv_path)
    df = pd.read_csv(csv_path)

    #Create list of balancing authorities
    ba_list = list(df.columns)
    ba_list.pop(0)

    #For every balancing authority, iterate over each different
    #fuel type, and if the value is >0, add the data to the lists
    subregions = []
    fuel_categories = []
    gen_ratios = []
    used = []
    for i in range(len(ba_list)):
        for j in range(len(df)):
            if df.iloc[j, i+1] > 0:
                subregions.append(ba_list[i])
                fuel_categories.append(df.iloc[j, 0])
                gen_ratios.append(df.iloc[j, i+1])
                used.append(False)

    # Put the lists into a dictionary to make into data frame
    # HOTFIX: fix AEO's fuel category names to match eLCI syntax
    AEO_dict = {REG_COL_NAME: subregions,
                FUEL_COL_NAME: [
                    convert_primary_fuel(x) for x in fuel_categories],
                GEN_COL_NAME: gen_ratios,
                USED_COL_NAME: used}

    return pd.DataFrame(AEO_dict)


def get_outlook_mix_description(csv_name):
    """Return the AEO grid mix description.

    Parameters
    ----------
    csv_name : string
        Name of AEO CSV file

    Returns
    -------
    str
        Description text for a given mix.
    """

    AEO_info = csv_name.split("_")

    model_year = AEO_info[0].lstrip('AEO')
    scenario = AEO_info[1]
    proj_year = AEO_info[2].rstrip('.csv')

    # URL included is for 2022 AEO forecasts. May eventually want to
    # change to be specific to model year
    r_txt = f'The balancing authority outlook mix is based on the AEO forecast '
    r_txt += f'made in {model_year}. This mix is for the \"{scenario}\" scenario in the '
    r_txt += f'year {proj_year}. More information about these scenarios can be found at: '
    r_txt += f'https://www.eia.gov/outlooks/archive/aeo22/assumptions/case_descriptions.php'

    return r_txt


def make_ba_dict():
    """Return a dictionary of balancing authority names and codes.

    These names and codes are defined in the supplementary CSV file,
    ba_codes.csv.

    Returns
    -------
    dict
        A dictionary where keys are balancing authority names and values
        are their associated short codes. Note that more than one name
        entry may map to the same short code.
    """
    df = read_ba_codes()
    ba_dict = dict()

    # Iterate through all rows in the data frame, adding
    # BA names as keys and BA codes as values to ba_dict
    # WARNING: duplicate names in the CSV will take on the
    #          last defined code.
    for ba_code, row in df.iterrows():
        ba_dict[row.BA_Name] = ba_code

    return ba_dict


def make_forecast_process_name(p_name, at_grid=True, is_gen=True):
    """Create a new forecast process name for generation (or consumption)
    at grid (or at user).

    Parameters
    ----------
    p_name : str
        process name (e.g., Electricity; at grid; generation mix)
    at_grid : bool, optional
        Whether name includes "at grid"; otherwise, "at user";
        defaults to True
    is_gen : bool, optional
        Whether names includes "generation"; otherwise, "consumption";
        defaults to True

    Returns
    -------
    str
        The same electricity generation grid mix process name, but with
        'forecast' added to the name.

    Raises
    ------
    ValueError
        For a process name that is not Electricity; at grid; generation mix
    """
    # Should return
    # "Electricity; at grid; forecast generation mix - BA NAME"
    g_txt = "at user"
    if at_grid:
        g_txt = "at grid"
    c_txt = "consumption"
    if is_gen:
        c_txt = "generation"
    q = re.compile("^(Electricity; %s;)( %s mix - .*)$" % (g_txt, c_txt))
    if q.match(p_name):
        return q.sub("\\1 forecast\\2", p_name)
    else:
        raise ValueError(
            "Expected Electricity; %s; %s process, found '%s'" % (
                g_txt, c_txt, p_name))


def make_outlook_gen(netl, pid, ba, df, csv_name, ba_dict):
    """Two-step process to make an at grid, outlook generation mix process.

    1. copy old process and update exchange vals
    2. add new outlook process to project

    Parameters
    ----------
    netl : NetlOlca
        An NetlOlca class connected to an openLCA project.
    pid : str
        A universally unique identifier (UUID) for an Electricity; at grid;
        generation mix process.
    ba : str
        Balancing Authority name associated with the pid.
    df : pandas.DataFrame
        Data frame containing outlook grid mixes for all BAs
    csv_name : str
        Name of the csv containing AEO grid mixes
    ba_dict : dictionary
        Dictionary containing BA names as keys and BA codes as values


    Returns
    -------
    tuple
        A tuple of length two.

        -   str, the universally unique identifier for the new outlook mix
            process
        -   pandas.DataFrame, the data frame with new AEO mixes with updated
            'Used' column (indicates if a fuel and subregion was found and
            updated in the openLCA database).

    Notes
    -----
    WARNING: this can and will create multiple versions of the
    'Electricity; at grid; outlook generation mix' process---one for each
    time this method is run. The description text has additional info on
    which of the four mix options was chosen for calculating outlook mixes.
    """
    # Creates the new process with updated exchanges
    p_new, df = update_exchange_to_outlook(netl, pid, ba, df, csv_name, ba_dict)
    # Pushes the changes to the openLCA database/JSON-LD
    is_okay = netl.add(p_new)
    new_id = find_new_process_id(netl, p_new.name)

    if not is_okay or new_id is None:
        raise IOError(
            "Failed to update the openLCA database with new process!"
        )

    return (new_id, df)


def run(con, json_file, csv_dir, csv_name):
    """The main run method.

    Connects to openLCA project, finds 'Electricity; at grid; generation mix'
    processes, replaces the generation mix with the outlook grid mix data,
    adds the new outlook generation mix process to the project, and creates
    new 'Electricity; at grid, consumption outlook mix' processes by updating
    the original providers to the new at-grid outlook generation mix processes
    that were just created.

    The user is required to update the 'Electricity; at user; consumption mix'
    process providers, which can now point to the 'Electricity; at grid;
    consumption mix' or 'Electricity; at grid; consumption outlook mix'
    processes.

    Parameters
    ----------
    con : int
        Connection type. 1: IPC-Server  2: JSON-LD
    json_file : str
        Relative (or absolute) path to JSON-LD project file
        (only relevant if connection type 2 is selected)
    csv_dir : str
        Folder path to where outlook grid mix CSV data files are located
    csv_name : str
        Name of the csv containing AEO grid mixes
    """
    # Establish connection to openLCA project
    netl = NetlOlca()
    if con == 1:
        logging.info("Establishing connection to openLCA project")
        netl.connect()
    else:
        logging.info("Opening openLCA project file")
        netl.open(jsonld_file=json_file)
    netl.read()

    # Read in AEO CSV
    df = get_outlook_mix(csv_dir, csv_name)

    # A map between BA names and their respective codes.
    logging.info("Reading balancing authority name-to-code map")
    ba_dict = make_ba_dict()

    my_matches = netl.get_electricity_gen_processes()
    ba_ids = {} # for each BA, store original and outlook process UIDs
    logging.info("Creating forecast generation mix processes")
    for m in my_matches:
        uid, name = m
        rid, df = make_outlook_gen(netl, uid, name, df, csv_name, ba_dict)
        ba_ids[uid] = rid

    # Create the consumption outlook mixes, linking them to their new
    # outlook generation mix processes.
    q1 = re.compile("^Electricity; at grid; consumption mix - US - US$")
    q2 = re.compile("^Electricity; at grid; consumption mix - .* - FERC$")
    q3 = re.compile("^Electricity; at grid; consumption mix - .* - BA$")

    logging.info("Creating forecast consumption mix processes")
    update_providers(netl, q1, ba_ids)
    update_providers(netl, q2, ba_ids)
    update_providers(netl, q3, ba_ids)

    # Gracefully close established connections
    logging.info("Disconnecting from project.")
    if con == 1:
        netl.disconnect()
    else:
        netl.close()


def update_exchange_to_outlook(netl, pid, ba, df, csv_name, ba_dict):
    """Create new electricity at grid outlook generation mix process for a
    given balancing authority area.

    Based on olca-schema version 2, exchanges are the flow amounts into and
    out of a process. This method iterates over each exchange, which, for
    generation mixes at grid are the different fuel generation processes.
    Each fuel has an amount associated to a BA's generation mix; these amounts
    should add to one (i.e., mix fractions). The exchange amounts are replaced
    with the AEO mixes (as defined in the CSV files produced by
    scenario modeler's AEO Python class).

    Parameters
    ----------
    netl : NetlOlca
        An NetlOlca class connected to an openLCA project.
    pid : str
        A universally unique identifier (UUID) for an Electricity; at grid;
        generation mix process.
    ba : str
        Balancing Authority name associated with the pid.
    df : pandas.DataFrame
        Data frame containing outlook grid mixes for all BAs
    csv_name : str
        Name of the csv containing AEO grid mixes
    ba_dict : dictionary
        Dictionary containing BA names as keys and BA codes as values

    Returns
    -------
    olca_schema.Process
        A new openLCA Process class.

    Notes
    -----
    Fuel categories that are not found in the original data set are
    created as flows without providers.
    """

    # Define query for searching fuel category from exchange description
    q = re.compile("^from (\\w+) - (.*)$")

    # Read the outlook mix dataset and its respective description text
    logging.debug("Reading outlook mix data from file")

    p_desc = (
        "Electricity generation mixes updated to reflect AEO forecast mix "
        "(https://www.eia.gov/outlooks/aeo/). ")
    p_desc += get_outlook_mix_description(csv_name)

    # Create new forecast mix process
    logging.info("Creating new process for %s" % ba)
    p_new = get_new_process(netl, pid, p_desc)

    # Query for fuels associated with the given BA area
    try:
        ba_code = ba_dict[ba]
    except KeyError:
        # If a BA code is not found, this will be skipped
        ba_code = 'BA CODE NOT FOUND'
        logging.warning('Balancing authority not found in BA Codes CSV. '
                        f'Add \"{ba}\" to BA Codes CSV with '
                        'corresponding BA code.')

    b = df.query("`%s` == '%s'" % (REG_COL_NAME, ba_code))

    # NOTE: Currently this method, if it has all zeros, it will overwrite
    # previous data
    # TODO: Make it so that this is skipped if somehow a BA has all 0 values.
    # Make a test.

    # NOTE: For electricity grid mixes, there are always two or more exchanges:
    # one output and X inputs. The inputs have the grid mix values we want.
    # Initialize variables which will be used as reference.
    flow = None
    flow_property = None
    unit = None
    unassigned = True
    for p_ex in p_new.exchanges:
        if p_ex.is_input:
            if unassigned:
                # Assign reference variables
                flow = p_ex.flow
                flow_property = p_ex.flow_property
                unit = p_ex.unit
                unassigned = False

            # Query for fuel name
            f_name = ""
            r = q.match(p_ex.description)
            if r:
                f_name = r.group(1)

            # Query data for new mix
            a = b.query("`%s` == '%s'" % (FUEL_COL_NAME, f_name))

            # Change row to indicate it is being read
            df.loc[(df[REG_COL_NAME] == ba_code) & (df[FUEL_COL_NAME] == f_name), USED_COL_NAME] = True

            # Update mix amounts
            if len(a) == 1:
                # Best case scenario; set new mix amount
                new_mix = a.iloc[0].Gen_Ratio_new
                logging.info("Replacing %s with %s for %s" % (
                    p_ex.amount, new_mix, f_name))
                p_ex.amount = new_mix
            elif len(a) == 0 and len(b) == 0:
                # Failed to find BA in the 2020 dataset.
                # Could be that there is just no REC data to remove.
                # For the time being, keep it, because we're none the wiser.
                logging.info("Failed to find %s; skipping" % ba)
            elif len(a) == 0:
                # Failed to find fuel for a known BA; set to zero.
                # TODO: consider removing this exchange from exchanges list
                logging.info("Zeroing mix for %s" % f_name)
                p_ex.amount = 0.0
            else:
                # This is a bad place to be.
                logging.warning(
                    "Found multiple matches for %s for %s!" % (f_name, ba))

    # Add new flows for unused fuels with updated use column
    b = df.query("`%s` == '%s'" % (REG_COL_NAME, ba_code))
    a = b[~b[USED_COL_NAME]]

    # Generate and append each new exchange that was not originally part
    # of the process
    for _, row in a.iterrows():
        new_ex = netl.make_exchange()
        new_ex.amount = row[GEN_COL_NAME]
        new_ex.description = f'from {row[FUEL_COL_NAME]} - {ba}'
        new_ex.internal_id = len(p_new.exchanges) + 1
        new_ex.is_avoided_product = False
        new_ex.is_input = True
        new_ex.is_quantitative_reference = False
        new_ex.flow = flow
        new_ex.flow_property = flow_property
        new_ex.unit = unit
        p_new.exchanges.append(new_ex)

    return (p_new, df)


def update_providers(netl, q, b_dict):
    """Iterates over processes, updates their default providers based on a
    look-up dictionary of UUIDs, and adds the new 'outlook' process to the
    open project.

    Notes
    -----
    If, for any reason, any of the exchange processes do not have a outlook
    mix counterpart (e.g., undefined or Canadian), then the new outlook
    mix process is not created (e.g., 'Electricity; at grid; outlook
    consumption mix - US - US' is not created unless all exchange processes
    have a 'Electricity; at grid; outlook generation mix' process associated
    with them).

    The outlook generation mixes should all be at the BA level (even for US
    and FERC regions, except for 'Electricity; at grid; consumption mix - US -
    LCI', which has inventory data). The LCI consumption mix should get skipped
    due to the failed search against the keys in ``b_dict``.

    Parameters
    ----------
    netl : NetlOlca
        Instance of NetlOlca connected to an openLCA project.
    q : re.Pattern
        A regular expression pattern object used to match process names.
    b_dict : dict
        A dictionary where keys are process UUIDs associated with Electricity
        at grid; generation mixes at the Balancing Authority level and keys
        are the process UUIDs for their outlook mix counterpart.
    """
    r = netl.match_process_names(q)
    for m in r:
        try:
            # Iterate over each process exchange, search for outlook process
            # (based on the ba_ids created above), update default provider.
            uid, name = m
            d_str = "Default providers updated to forecast generation mix."
            p_new = get_new_process(netl, uid, d_txt=d_str, is_gen=False)
            n_ex = len(p_new.exchanges)
            logging.info("Updating %d exchanges for '%s'" % (n_ex, name))
            for i in range(n_ex):
                p_ex = p_new.exchanges[i]
                # Skip output flows.
                if p_ex.is_input:
                    dp_id = p_ex.default_provider.id
                    rp_id = b_dict[dp_id]     # throws error when not found!

                    # Update default provider to outlook mix reference object
                    rp_obj = netl.query(netl.get_spec_class("Process"), rp_id)
                    if rp_obj:
                        p_new.exchanges[i].default_provider = rp_obj.to_ref()
                    else:
                        e_str = "Failed to find outlook process '%s'" % rp_id
                        logging.error(e_str)
                        raise KeyError(e_str)
        except KeyError:
            logging.warning("Failed to make outlook process for '%s'" % name)
            pass
        else:
            # Add p_new to project
            netl.add(p_new)


##############################################################################
# MAIN
##############################################################################
if __name__ == '__main__':

    # Libraries required for CLI only
    import argparse

    # Set up logger
    root_logger = logging.getLogger()
    root_handler = logging.StreamHandler()
    rec_format = (
        "%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(funcName)s:"
        "%(message)s")
    formatter = logging.Formatter(rec_format, datefmt='%Y-%m-%d %H:%M:%S')
    root_handler.setFormatter(formatter)
    root_logger.addHandler(root_handler)

    # Add command-line argument handling to turn this into a tool.
    p = argparse.ArgumentParser(
        description="The AEO grid mix replacer.")
    p.add_argument(
        "-c", "--connection", default=3, choices=[1, 2], type=int,
        help="1: IPC-Server  2: JSON-LD")
    p.add_argument(
        "-d", "--directory", default="data",
        help="folder path containing AEO mix CSV files")
    p.add_argument(
        "-f", "--file_name", default="", type=str,
        help="name of AEO mix csv file")
    p.add_argument(
        "-p", "--p_file", default="",
        help="JSON-LD project file (optional)")
    p.add_argument(
        "-l", "--log_level", default="INFO",
        choices=['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='logging level, defaults to INFO')

    # Read arguments:
    args = p.parse_args()

    # Manage command-line arguments
    root_handler.setLevel(args.log_level)
    if not os.path.isdir(args.directory):
        raise OSError("Could not find data folder, '%s'" % args.directory)
    root_logger.info("Running...")
    run(
        con=args.connection,
        json_file=args.p_file,
        csv_dir=args.directory,
        csv_name=args.file_name,
        #gen_yr=args.year
    )
    root_logger.info("... complete!")
