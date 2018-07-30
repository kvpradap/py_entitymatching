# coding=utf-8
import logging
import math

import dask
from dask import delayed
from dask.diagnostics import ProgressBar
import pandas as pd
from numpy.random import RandomState



import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.sampler.down_sample import _inv_index, _probe_index_split, get_num_procs
from py_entitymatching.utils.catalog_helper import log_info
from py_entitymatching.utils.validation_helper import validate_object_type

logger = logging.getLogger(__name__)


def dask_down_sample(table_a, table_b, size, y_param, show_progress=True,
                     verbose=False, seed=None, rem_stop_words=True,
                     rem_puncs=True, n_jobs=1):
    """
    WARNING: THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR OWN RISK
    
    This function down samples two tables A and B into smaller tables A' and
    B' respectively.

    Specifically, first it randomly selects `size` tuples
    from the table B to be table B'. Next, it builds an inverted index I
    (token, tuple_id) on table A. For each tuple x ∈ B', the algorithm
    finds a set P of k/2 tuples from I that match x,
    and a set Q of k/2 tuples randomly selected from A - P.
    The idea is for A' and B' to share some matches yet be
    as representative of A and B as possible.

    Args:
        table_a,table_b (DataFrame): The input tables A and B.
        size (int): The size that table B should be down sampled to.
        y_param (int): The parameter to control the down sample size of table A.
            Specifically, the down sampled size of table A should be close to
            size * y_param.
        show_progress (boolean): A flag to indicate whether a progress bar
            should be displayed (defaults to True).
        verbose (boolean): A flag to indicate whether the debug information
         should be displayed (defaults to False).
        seed (int): The seed for the pseudo random number generator to select
            the tuples from A and B (defaults to None).
        rem_stop_words (boolean): A flag to indicate whether a default set of stop words 
         must be removed.
        rem_puncs (boolean): A flag to indicate whether the punctuations must be 
         removed from the strings.
        n_jobs (int): The number of parallel jobs to be used for computation
            (defaults to 1). If -1 all CPUs are used. If 0 or 1,
            no parallel computation is used at all, which is useful for
            debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are
            used (where n_cpus is the total number of CPUs in the
            machine). Thus, for n_jobs = -2, all CPUs but one are used.
            If (n_cpus + 1 + n_jobs) is less than 1, then no parallel
            computation is used (i.e., equivalent to the default).


    Returns:
        Down sampled tables A and B as pandas DataFrames.

    Raises:
        AssertionError: If any of the input tables (`table_a`, `table_b`) are
            empty or not a DataFrame.
        AssertionError: If `size` or `y_param` is empty or 0 or not a
            valid integer value.
        AssertionError: If `seed` is not a valid integer
            value.
        AssertionError: If `verbose` is not of type bool.
        AssertionError: If `show_progress` is not of type bool.
        AssertionError: If `n_jobs` is not of type int.

    Examples:
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
        >>> sample_A, sample_B = em.down_sample(A, B, 500, 1, n_jobs=-1)

        # Example with seed = 0. This means the same sample data set will be returned
        # each time this function is run.
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
        >>> sample_A, sample_B = em.dask_down_sample(A, B, 500, 1, seed=0, n_jobs=-1)
    """

    logger.warning("WARNING: THIS COMMAND IS EXPERIMENTAL AND NOT TESTED. USE AT YOUR "
                   "OWN RISK.")

    if not isinstance(table_a, pd.DataFrame):
        logger.error('Input table A is not of type pandas DataFrame')
        raise AssertionError(
            'Input table A is not of type pandas DataFrame')

    if not isinstance(table_b, pd.DataFrame):
        logger.error('Input table B is not of type pandas DataFrame')
        raise AssertionError(
            'Input table B is not of type pandas DataFrame')

    if len(table_a) == 0 or len(table_b) == 0:
        logger.error('Size of the input table is 0')
        raise AssertionError('Size of the input table is 0')

    if size == 0 or y_param == 0:
        logger.error(
            'size or y cannot be zero (3rd and 4th parameter of downsample)')
        raise AssertionError(
            'size or y_param cannot be zero (3rd and 4th parameter of downsample)')

    if seed is not None and not isinstance(seed, int):
        logger.error('Seed is not of type integer')
        raise AssertionError('Seed is not of type integer')

    if len(table_b) < size:
        logger.warning(
            'Size of table B is less than b_size parameter - using entire table B')


    validate_object_type(verbose, bool, 'Parameter verbose')
    validate_object_type(show_progress, bool, 'Parameter show_progress')
    validate_object_type(rem_stop_words, bool, 'Parameter rem_stop_words')
    validate_object_type(rem_puncs, bool, 'Parameter rem_puncs')
    validate_object_type(n_jobs, int, 'Parameter n_jobs')

    # get and validate required metadata
    log_info(logger, 'Required metadata: ltable key, rtable key', verbose)



    s_inv_index = _inv_index(table_a, rem_stop_words, rem_puncs)

    # Randomly select size tuples from table B to be B'
    # If a seed value has been give, use a RandomState with the given seed
    b_sample_size = min(math.floor(size), len(table_b))
    if seed is not None:
        rand = RandomState(seed)
    else:
        rand = RandomState()
    b_tbl_indices = list(rand.choice(len(table_b), int(b_sample_size), replace=False))

    n_jobs = get_num_procs(n_jobs, len(table_b))

    sample_table_b = table_b.ix[b_tbl_indices]
    if n_jobs <= 1:
        # Probe inverted index to find all tuples in A that share tokens with tuples in B'.
        s_tbl_indices = _probe_index_split(sample_table_b, y_param,
                                           len(table_a), s_inv_index, show_progress,
                                           seed, rem_stop_words, rem_puncs)
    else:
        sample_table_splits = pd.np.array_split(sample_table_b, n_jobs)
        results = []
        for job_index in range(n_jobs):
            partial_result = delayed(_probe_index_split)(sample_table_splits[job_index],
                                                         y_param, len(table_a),
                                                         s_inv_index,
                                                         # set show_progress=False as
                                                         # we use dask progressbar
                                                         False, seed,                                                                                                           rem_stop_words, rem_puncs)
            results.append(partial_result)
        results = delayed(wrap_results)(results)
        if show_progress:
            with ProgressBar():
                results = results.compute(scheduler="processes", num_workers=n_jobs)
        else:
            results = results.compute(scheduler="processes", num_workers=n_jobs)

        s_tbl_indices = postprocess_results(results)

    s_tbl_indices = list(s_tbl_indices)
    l_sampled = table_a.iloc[list(s_tbl_indices)]
    r_sampled = table_b.iloc[list(b_tbl_indices)]

    # update catalog
    if cm.is_dfinfo_present(table_a):
        cm.copy_properties(table_a, l_sampled)
    if cm.is_dfinfo_present(table_b):
        cm.copy_properties(table_b, r_sampled)

    return l_sampled, r_sampled


def wrap_results(results):
    return results
def postprocess_results(results):
    results = map(list, results)
    s_tbl_indices = set(sum(results, []))
    return s_tbl_indices
