#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# residual_grid_mix.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import logging
import os
import re

import pandas as pd

from netlolca.NetlOlca import NetlOlca


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module is a part of a project funded by the United States
Department of Energy National Energy Technology Laboratory.

This module provides a run method designed to connect to an openLCA project
(either directly via IPC-Server or indirectly via JSON-LD) and create residual
grid mix processes for electricity generation at Balancing Authority areas.

The run method takes several arguments that can either be run directly via a
method call or via the command line using the CLI parameters:

    -c {1,2}, --connection {1,2}
        1: IPC-Server 2: JSON-LD
    -r RES_DATA, --res_data RES_DATA
        folder path containing residual mix CSV files
    -m {1,2,3,4}, --mix {1,2,3,4}
        residual mix file (one of four options)
    -y YEAR, --year YEAR
        year associated with electricity generation, defaults to 2020
    -p P_FILE, --p_file P_FILE
        JSON-LD file (optional)

Examples
--------
From within Python:

>>> run(con=1, json_file="", csv_dir="data", mix_opt=1, gen_yr=2016)

From the command line:

$ python run.py -c 1 -r data -m 1 -y 2016

Notes
-----
Warning!

Multiple runs of the run() method on the same openLCA project will result
in multiple instances of "Electricity; at grid; residual generation mix"
processes, whether or not they represent different residual mix methods.

If you need/want to update existing residual mix processes, you should
delete old ones first!

Quality control tests:

1.  For each new residual grid mix process, check that input exchange
    amounts sum to output.
2.  Check for multiple instances of the same process name (e.g., if run.py
    was executed multiple times on the same project).

The 2016 electricity baseline is organized into several tiers. Those of
interest are:

-   'Electricity; at user; consumption mix - ' US - US / x - FERC / y - BA
    (converts flow from 2300V to 120 V; includes loss value; not updated)

    -   'Electricity; at grid; consumption mix - ' US - US / x - FERC /
        y - BA (account for electricity trading; copied as residual mix)

        -   'Electricity; at grid; generation mix - xxx - BA'
            (based on primary fuels; updated to residual mix)

            -   'Electricity - FUEL CATEGORY - xxx'
                (BA inventory from primary fuel generation; no change)

Changelog:
    v.1.1.0:
        -   Introduce 'at_grid' and 'is_gen' parameters to
            ``make_residual_process_name`` method.
        -   Abstract methods in run to new ``make_residual_gen`` method.
        -   Create new ``update_providers`` method for creating new at grid
            residual consumption mix processes within an openLCA project.
        -   Add the three levels of 'at grid' consumption mix processes to be
            updated to residual processes in ``run``.
        -   Change default mix option to '4', corresponding to zeroing excess
            REC generation and using the facility count aggregation method.

Version:
    1.1.0
Last Edited:
    2025-01-21
"""
__all__ = [
    'FUEL_COL_NAME',
    'REG_COL_NAME',
    'get_new_process',
    'get_residual_mix',
    'get_residual_mix_description',
    'make_residual_gen',
    'make_residual_process_name',
    'run',
    'test',
    'test_s1',
    'test_s2',
    'test_s3',
    'update_exchange_to_residual',
    'update_providers',
]


##############################################################################
# GLOBALS
##############################################################################
REG_COL_NAME = "Subregion"
'''str : Pandas data column for residual mix region name.'''
FUEL_COL_NAME = "FuelCategory"
'''str : Pandas data column for residual mix fuel name.'''


##############################################################################
# FUNCTIONS
##############################################################################
def get_residual_mix(u_choice, g_year, d_dir):
    """Read residual mix data file.

    Parameters
    ----------
    u_choice : int
        User choice of data file (there are four options, see notes)
    g_year : int
        Year associated with electricity generation (e.g., 2016 or 2020)
    d_dir : str
        Data folder where residual grid mix CSV files are located.

    Returns
    -------
    pandas.DataFrame
        Residual grid mix data at the Balancing Authority level.

    Raises
    ------
    IndexError
        For user choice outside valid range (1--4)
    OSError
        For missing data file (check data folder path)

    Notes
    -----
    The four residual grid mix CSV files are generated by running the
    `elci_to_rem` Python tool. Copy the CSV files to a data folder without
    changing their file names, which, for 2020, should be:

    1. res-mix_2020_rec-keep_agg-area.csv
    2. res-mix_2020_rec-keep_agg-count.csv
    3. res-mix_2020_rec-zero_agg-area.csv
    4. res-mix_2020_rec-zero_agg-count.csv
    """
    csv_files = {
        1: f"res-mix_{g_year}_rec-keep_agg-area.csv",
        2: f"res-mix_{g_year}_rec-keep_agg-count.csv",
        3: f"res-mix_{g_year}_rec-zero_agg-area.csv",
        4: f"res-mix_{g_year}_rec-zero_agg-count.csv"
    }
    if u_choice not in csv_files.keys():
        raise IndexError("Choice %s not found!" % u_choice)
    csv_file = csv_files[u_choice]
    csv_path = os.path.join(d_dir, csv_file)
    if not os.path.isfile(csv_path):
        raise OSError("Missing file, %s" % csv_path)
    return pd.read_csv(csv_path)


def get_residual_mix_description(u_choice):
    """Return the residual grid mix description based on user choice.

    Parameters
    ----------
    u_choice : int
        User choice of data file
        (there are four options; see :func:`get_residual_mix`).

    Returns
    -------
    str
        Description text for a given mix.
    """
    r_txt = ""
    if u_choice == 1:
        r_txt += (
            "The balancing authority residual mix is based on an areal "
            "weighting method of state-level REC sales where excess REC "
            "generation amounts (MWh) are subtracted from non-renewables, "
            "assuming that some renewable energy may be provided from a "
            "non-renewable fuel category (e.g., mixed/other fuels).")
    elif u_choice == 2:
        r_txt += (
            "The balancing authority residual mix is based on a facility "
            "count weighting method of state-level REC sales where excess REC "
            "generation amounts (MWh) are subtracted from non-renewables, "
            "assuming that some renewable energy may be provided from a "
            "non-renewable fuel category (e.g., mixed/other fuels).")
    elif u_choice == 3:
        r_txt += (
            "The balancing authority residual mix is based on an areal "
            "weighting method of state-level REC sales where excess REC "
            "generation amounts (MWh) are ignored (i.e., assumed zero; "
            "accounts for all available renewable generation).")
    elif u_choice == 4:
        r_txt += (
            "The balancing authority residual mix is based on a facility "
            "count weighting method of state-level REC sales where excess REC "
            "generation amounts (MWh) are ignored (i.e., assumed zero; "
            "accounts for all available renewable generation).")
    return r_txt


def make_residual_process_name(p_name, at_grid=True, is_gen=True):
    """Create a new process name for residual generation at grid.

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
        'residual' added to the name.

    Raises
    ------
    ValueError
        For a process name that is not Electricity; at grid; generation mix
    """
    # Should return
    # "Electricity; at grid; residual generation mix - BA NAME"
    g_txt = "at user"
    if at_grid:
        g_txt = "at grid"
    c_txt = "consumption"
    if is_gen:
        c_txt = "generation"
    q = re.compile("^(Electricity; %s;)( %s mix - .*)$" % (g_txt, c_txt))
    if q.match(p_name):
        return q.sub("\\1 residual\\2", p_name)
    else:
        raise ValueError(
            "Expected Electricity; %s; %s process, found '%s'" % (
                g_txt, c_txt, p_name))


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
        p_dict['name'] = make_residual_process_name(p.name, at_grid, is_gen)
        # TODO: Consider updating the Process documentation
        # p_dict['processDocumentation']['validFrom'] = '2020-01-01'
        # p_dict['processDocumentation']['validUntil'] = '2020-12-31'

    # Update existing description or create new
    if isinstance(p_dict['description'], str):
        p_dict['description'] += " "
        p_dict['description'] += d_txt
    else:
        p_dict['description'] = d_txt

    return n.get_spec_class("Process").from_dict(p_dict)


def make_residual_gen(netl, pid, ba, data_dir, m, y):
    """Two-step process to make an at grid, residual generation mix process.

    1. copy old process and update exchange vals
    2. add new residual process to project

    Parameters
    ----------
    netl : NetlOlca
        An NetlOlca class connected to an openLCA project.
    pid : str
        A universally unique identifier (UUID) for an Electricity; at grid;
        generation mix process.
    ba : str
        Balancing Authority name associated with the pid.
    data_dir : str
        Folder path where residual grid mix CSV files are located.
    m : int
        Residual mix option (four choices; see :func:`get_residual_mix` for
        details).
    y : int
        Year associated with electricity generation (e.g., 2016 or 2020).

    Returns
    -------
    str
        The universally unique identifier for the new residual mix process.

    Notes
    -----
    WARNING: this can and will create multiple versions of the
    'Electricity; at grid; residual generation mix' process---one for each
    time this method is run. The description text has additional info on
    which of the four mix options was chosen for calculating residual mixes.
    """
    p_new = update_exchange_to_residual(netl, pid, ba, data_dir, m, y)
    netl.add(p_new)
    return p_new.id


def run(con, json_file, csv_dir, mix_opt, gen_yr):
    """The main run method.

    Connects to openLCA project, finds 'Electricity; at grid; generation mix' processes, replaces the generation mix with the residual grid mix data
    (based on the mix_opt and gen_yr parameters), adds the new residual
    generation mix process to the project, and creates new 'Electricity; at
    grid, consumption residual mix' processes by updating the original
    providers to the new at grid residual generation mix processes that were
    just created.

    The user is required to update the 'Electricity; at user; consumption mix'
    process providers, which can now point to the 'Electricity; at grid;
    consumption mix' or 'Electricity; at grid; consumption residual mix'
    processes.

    Parameters
    ----------
    con : int
        Connection type. 1: IPC-Server  2: JSON-LD
    json_file : str
        Relative (or absolute) path to JSON-LD project file
        (only relevant if connection type 2 is selected)
    csv_dir : str
        Folder path to where residual grid mix CSV data files are located
    mix_opt : int
        The residual grid mix option (four choices, see
        :func:`get_residual_mix` for details)
    gen_yr : int
        The year associated with electricity generation (e.g., 2016 or 2020)
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

    my_matches = netl.get_electricity_gen_processes()
    ba_ids = {} # for each BA, store original and residual process UIDs
    for m in my_matches:
        uid, name = m
        rid = make_residual_gen(netl, uid, name, csv_dir, mix_opt, gen_yr)
        ba_ids[uid] = rid

    # Create the consumption residual mixes, linking them to their new
    # residual generation mix processes.
    q1 = re.compile("^Electricity; at grid; consumption mix - US - US$")
    q2 = re.compile("^Electricity; at grid; consumption mix - .* - FERC$")
    q3 = re.compile("^Electricity; at grid; consumption mix - .* - BA$")
    update_providers(netl, q1, ba_ids)
    update_providers(netl, q2, ba_ids)
    update_providers(netl, q3, ba_ids)

    # Gracefully close established connections
    logging.info("Disconnecting from project.")
    if con == 1:
        netl.disconnect()
    else:
        netl.close()


def update_providers(netl, q, b_dict):
    """Iterates over processes, updates their default providers based on a
    look-up dictionary of UUIDs, and adds the new 'residual' process to the
    open project.

    Notes
    -----
    If, for any reason, any of the exchange processes do not have a residual
    mix counterpart (e.g., undefined or Canadian), then the new residual
    mix process is not created (e.g., 'Electricity; at grid; residual
    consumption mix - US - US' is not created unless all exchange processes
    have a 'Electricity; at grid; residual generation mix' process associated
    with them).

    The residual generation mixes should all be at the BA level (even for US
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
        are the process UUIDs for their residual mix counterpart.
    """
    r = netl.match_process_names(q)
    for m in r:
        try:
            # Iterate over each process exchange, search for residual process
            # (based on the ba_ids created above), update default provider.
            uid, name = m
            d_str = "Default providers updated to residual generation mix."
            p_new = get_new_process(netl, uid, d_txt=d_str, is_gen=False)
            n_ex = len(p_new.exchanges)
            logging.info("Updating %d exchanges for '%s'" % (n_ex, name))
            for i in range(n_ex):
                p_ex = p_new.exchanges[i]
                # Skip output flows.
                if p_ex.is_input:
                    dp_id = p_ex.default_provider.id
                    rp_id = b_dict[dp_id]     # throws error when not found!

                    # Update default provider to residual mix reference object
                    rp_obj = netl.query(netl.get_spec_class("Process"), rp_id)
                    if rp_obj:
                        p_new.exchanges[i].default_provider = rp_obj.to_ref()
                    else:
                        e_str = "Failed to find residual process '%s'" % rp_id
                        logging.error(e_str)
                        raise KeyError(e_str)
        except KeyError:
            logging.warning("Failed to make residual process for '%s'" % name)
            pass
        else:
            # Add p_new to project
            netl.add(p_new)


def test(con, json_file, csv_dir, mix_opt, gen_yr):
    """Quality control unit tests for residual grid mix replacement tool.

    Parameters
    ----------
    con : int
        Connection type. 1: IPC-Server  2: JSON-LD
    json_file : str
        Relative (or absolute) path to JSON-LD project file
        (only relevant if connection type 2 is selected)
    csv_dir : str
        Folder path to where residual grid mix CSV data files are located
    mix_opt : int
        The residual grid mix option (four choices, see
        :func:`get_residual_mix` for details)
    gen_yr : int
        The year associated with electricity generation (e.g., 2016 or 2020)

    Returns
    -------
    list
        List of booleans, if true, then each test passed the quality review.
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

    # Query for residual grid mix processes
    my_matches = netl.get_electricity_gen_processes(residual=True)

    # Test 1: Find duplicate Balancing Authorities
    t1 = test_s1(my_matches)

    # Test 2: See if exchange inputs sum to outputs.
    t2, _ = test_s2(netl, my_matches)

    # Test 3: See if there are unaccounted for fuel types in the new process
    t3, _ = test_s3(netl, my_matches, csv_dir, mix_opt, gen_yr)

    # Gracefully close established connections
    if con == 1:
        netl.disconnect()
    else:
        netl.close()

    return [t1, t2, t3]


def test_s1(p_list):
    """Duplicate balancing authority test.

    Parameters
    ----------
    p_list : list
        List of process UUIDs and Balancing Authority names associated with
        residual generation mixes within the openLCA project.

    Returns
    -------
    bool
    """
    no_dups = True
    ba_list = []
    for p in p_list:
        _, name = p
        if name not in ba_list:
            ba_list.append(name)
        else:
            print("Found duplicate BA, '%s" % name)
            no_dups = False
    return no_dups


def test_s2(n_obj, p_list):
    """Input/output exchange balance test.

    Parameters
    ----------
    n_obj : NetlOlca
        Instance of NetlOlca class connected to an openLCA project.
    p_list : list
        List of process UUIDs and Balancing Authority names associated with
        residual generation mixes within the openLCA project.

    Returns
    -------
    tuple
        Tuple of length two (bool, dict).
        The boolean is if the test passed.
        The dictionary contains the input, output, and difference totals for
        each balancing authority.
    """
    r_dict = {}
    thresh = 0.001
    is_okay = True
    for p in p_list:
        uid, name = p
        r_dict[name] = {'inputs': 0, 'outputs': 0, 'diff': 9999}
        p_obj = n_obj.query(n_obj.get_spec_class("Process"), uid)
        if p_obj is not None:
            for p_ex in p_obj.exchanges:
                if p_ex.is_input:
                    r_dict[name]['inputs'] += p_ex.amount
                else:
                    r_dict[name]['outputs'] += p_ex.amount
        r_dict[name]['diff'] = r_dict[name]['inputs'] - r_dict[name]['outputs']
        if abs(r_dict[name]['diff']) > thresh:
            is_okay = False
            print("Input/output inconsistency with %s" % name)
    return (is_okay, r_dict)


def test_s3(n_obj, p_list, d_dir, m_opt, g_yr):
    """Unaccounted generation fuel type test.

    Parameters
    ----------
    n_obj : NetlOlca
        An NetlOlca class instance connected to an openLCA project.
    p_list : list
        List of process UUIDs and Balancing Authority names associated with
        residual generation mixes within the openLCA project.
    d_dir : str
        Folder path to where residual grid mix CSV data files are located.
    m_opt : int
        The residual grid mix option.
    g_yr : int
        The year associated with electricity generation (e.g., 2016 or 2020).

    Returns
    -------
    tuple
        Tuple of length two (bool, dict).
        The boolean is whether the test passed.
        The dictionary contains keys for each balancing authority name and
        values are dictionaries with lists of fuel types associated with
        being 'replaced,' 'zeroed,' or 'missed.'

        -   Replaced means that the fuel category was found in both the
            unit process exchange list and the residual mix dataset.
        -   Zeroed means that the fuel category was found in the unit process
            exchange list, but not in the residual mix dataset.
        -   Missed means that the fuel category was found in the residual mix
            dataset, but not in the unit process exchange list (i.e.,
            unaccounted for generation).
    """
    is_okay = True

    # Define query for searching fuel category from exchange description
    q = re.compile("^from (\\w+) - (.*)$")

    # Read the residual mix dataset and its respective description text
    df = get_residual_mix(m_opt, g_yr, d_dir)

    r_dict = {}
    for p_tup in p_list:
        uid, name = p_tup
        # Initialize Balancing Authority area sub-dictionary
        r_dict[name] = {'replaced': [], 'zeroed': [], 'missed': []}
        # Query data for fuel category data
        b = df.query("`%s` == '%s'" % (REG_COL_NAME, name))
        # Query project for process
        p = n_obj.query(n_obj.get_spec_class("Process"), uid)
        if p is not None:
            for p_ex in p.exchanges:
                if p_ex.is_input:
                    # Query for fuel name
                    f_name = ""
                    r = q.match(p_ex.description)
                    if r:
                        f_name = r.group(1)

                    # Query data for new mix
                    a = b.query("`%s` == '%s'" % (FUEL_COL_NAME, f_name))

                    if len(a) == 1:
                        r_dict[name]['replaced'].append(f_name)
                    elif len(a) == 0 and len(b) == 0:
                        # Failed to find BA in the 2020 dataset.
                        # Could be that there is just no REC data to remove.
                        pass
                    elif len(a) == 0:
                        # Failed to find fuel for a known BA; set to zero.
                        r_dict[name]['zeroed'].append(f_name)
                    else:
                        pass

        # Now check for any inputs not already existing:
        f_list = []
        f_list += r_dict[name]['replaced']
        f_list += r_dict[name]['zeroed']
        r_dict[name]['missed'] = [
            i for i in b[FUEL_COL_NAME].values if i not in f_list]
        if len(r_dict[name]['missed']):
            is_okay = False

    return (is_okay, r_dict)


def update_exchange_to_residual(netl, pid, ba, data_dir, m, y):
    """Create new electricity at grid residual generation mix process for a
    given balancing authority area.

    Based on olca-schema version 2, exchanges are the flow amounts into and
    out of a process. This method iterates over each exchange, which, for
    generation mixes at grid are the different fuel generation processes.
    Each fuel has an amount associated to a BA's generation mix; these amounts
    should add to one (i.e., mix fractions). The exchange amounts are replaced
    with the 2020 residual mixes (as defined in the CSV files produced by
    main.py in the "elci_to_rem" Python package).

    Parameters
    ----------
    netl : NetlOlca
        An NetlOlca class connected to an openLCA project.
    pid : str
        A universally unique identifier (UUID) for an Electricity; at grid;
        generation mix process.
    ba : str
        Balancing Authority name associated with the pid.
    data_dir : str
        Folder path where residual grid mix CSV files are located.
    m : int
        Residual mix option (four choices; see :func:`get_residual_mix` for
        details).
    y : int
        Year associated with electricity generation (e.g., 2016 or 2020).

    Returns
    -------
    olca_schema.Process
        A new openLCA Process class.

    Notes
    -----
    If `elci_to_rem` generation year does not match the generation year with
    the electricity dataset used in the current openLCA project, there is a
    good chance that the fuel categories will not match, which leads to an
    unbalanced process. It is highly suggested that the quality control tests
    be run after performing this.
    """
    # Define query for searching fuel category from exchange description
    q = re.compile("^from (\\w+) - (.*)$")

    # Read the residual mix dataset and its respective description text
    logging.debug("Reading residual mix data from file")
    df = get_residual_mix(m, y, data_dir)
    p_desc = (
        f"Electricity generation mixes updated to reflect {y} residual grid "
        "mix based on NREL's Status and Trends in the Voluntary Market "
        "(https://www.nrel.gov/analysis/green-power.html). ")
    p_desc += get_residual_mix_description(m)

    # Create new residual mix process
    logging.info("Creating new process for %s" % ba)
    p_new = get_new_process(netl, pid, p_desc)

    # Query for fuels associated with the given BA area
    b = df.query("`%s` == '%s'" % (REG_COL_NAME, ba))

    # NOTE: For electricity grid mixes, there are always two or more exchanges:
    # one output and X inputs. The inputs have the grid mix values we want.
    for p_ex in p_new.exchanges:
        if p_ex.is_input:
            # Query for fuel name
            f_name = ""
            r = q.match(p_ex.description)
            if r:
                f_name = r.group(1)

            # Query data for new mix
            a = b.query("`%s` == '%s'" % (FUEL_COL_NAME, f_name))

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
    return p_new


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
        description="The residual grid mix replacer.")
    p.add_argument(
        "-c", "--connection", default=3, choices=[1, 2], type=int,
        help="1: IPC-Server  2: JSON-LD")
    p.add_argument(
        "-r", "--res_data", default="data",
        help="folder path containing residual mix CSV files")
    p.add_argument(
        "-m", "--mix", default=4, choices=[1, 2, 3, 4], type=int,
        help="residual mix file (one of four options)")
    p.add_argument(
        "-y", "--year", type=int, default=2020,
        help="year associated with electricity generation, defaults to 2020")
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
    if not os.path.isdir(args.res_data):
        raise OSError("Could not find data folder, '%s'" % args.res_data)
    root_logger.info("Running...")
    run(
        con=args.connection,
        json_file=args.p_file,
        csv_dir=args.res_data,
        mix_opt=args.mix,
        gen_yr=args.year
    )
    root_logger.info("... complete!")

    # Quality control
    # 2023-08-29
    #  Input/output inconsistency with:
    #   - El Paso Electric Company
    #   - Avista Corporation
    #   - Western Area Power Administration - Desert Southwest Region
    #   - Portland General Electric Company
    #   - Tallahassee, City of
    #   - South Carolina Electric & Gas Company
    #   - Western Area Power Administration - Rocky Mountain Region
    #   - NorthWestern Corporation
    is_okay = test(
        args.connection, args.p_file, args.res_data, args.mix, args.year)
    root_logger.info("Process passed all checks... %s" % all(is_okay))
