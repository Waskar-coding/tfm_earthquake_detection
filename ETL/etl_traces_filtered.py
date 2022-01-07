#Description
"""
Applies an obspy filter to a raw trace and saves the modified version.

To execute this script you need to activate a conda environment with obpsy installed:

conda activate [obspy environment]

These are the arguments to execute the script:

python etl_traces_spectrum.py type_i type_f filter_type fre_min [freq_max]

    type_i: Initial type, an integer that references a trace type

    type_f: Target type, an integer that references a spectrum type

    filter_type: bandpass, bandstop, low_pass or highpass

    freq_min: filter minimum frequency

    freq_max: filter maximum frequency, only for bandpass and bandstop filters

Both types must be indexes in TRACE_LOCATION_MAP from load_db
"""

#Tags
__author__ = "Óscar Gómez Nonnato"
__date__ = "05/09/2021"

#Libraries
##Standard
import os
import csv
import sys

##Package
from joblib import Parallel, delayed
import obspy
import pandas as pd
import sqlite3

##Local
from extract_traces import extract_traces
from load_db import LOCAL_SEISMIC_DB, TRACE_LOCATION_MAP

#Obspy suppported filters
TWO_FREQ_FILTERS = ['bandpass', 'bandstop']
ONE_FREQ_FILTERS = ['low_pass', 'highpass']


#Getting filter arguments
def get_filter_args():
    ##Documentation
    """
    Description
    -----------
    Makes sure the filter type provided is valid, clasifies the filter
    type into a filter class (one or two frequency filters, class 0 or 1)
    and parses the script's filter arguments.
    
    Returns
    -------
    filter_args: list
        List of filter arguments, contains filter class, filter type,
        minimum frequency, and maximum frequency if the filter type is
        a one frequency filter (filter class 1)
    """
    
    
    ##Checking for non valid filter types
    f_type = sys.argv[3]
    if not(f_type in TWO_FREQ_FILTERS or f_type in ONE_FREQ_FILTERS):
        raise TypeError("Unsupported filter type: {}".format(f_type))
    
    ##Creating args list
    filter_args = [0, f_type, int(sys.argv[4])]
    if f_type in TWO_FREQ_FILTERS:
        filter_args[0] = 1
        filter_args.append(int(sys.argv[5]))
    
    return filter_args


#Filtering traces
def fc_etl_filter(script_path, type_i, type_f, filter_args, total_traces):
    ##Documentation
    """
    Description
    -----------
    Returns a filter function to process the traces by using 
    the script's arguments.
    
    Parameters
    ----------
    scripth_path: str
        This script's path
    type_i: int
        Trace initial type
    type_f: int
        Filtered trace type
    filter_args: list
        Filter arguments: filter_class, filter_type,
        freq_min, freq_max (optional)
    total_traces: int
        Total number of traces to process
    
    Returns
    -------
    outter_etl_filter: function
        Filter function to process the traces.
    """
    
    ##Creating filter function: Depends on filter_type argument
    filter_class = filter_args[0]
    filter_type = filter_args[1]
    if filter_class == 0:
        inner_etl_filter = lambda trace:\
            trace.filter(filter_type, freq=filter_args[2]).normalize()
    else:
        inner_etl_filter = lambda trace:\
            trace.filter(filter_type, freqmin=filter_args[2], freqmax=filter_args[3]).normalize()
    
    ##Creating an error-proof wrapper for the filter function
    def outter_etl_filter(trace_row):
        i, code, station, component, start, final, file = trace_row
        try:
            os.chdir(TRACE_LOCATION_MAP[0])
            trace = obspy.read(file)
            trace = inner_etl_filter(trace)
            os.chdir(TRACE_LOCATION_MAP[type_f])
            new_trace_name = '_'.join(file.split('.')[:-1]) + '_' + str(type_f) + '.mseed'
            trace.write(new_trace_name, format="MSEED")
            new_trace_location = os.path.join(TRACE_LOCATION_MAP[type_f], new_trace_name)
            print('{}_{}_{}: {}/{}'.format(code, station, component, i+1, total_traces))
            return (code, station, component, start, final, type_f, new_trace_location)
        except:
            os.chdir(script_path)
            with open('filtered_traces_' + str(type_f) + '.csv', 'a', newline='') as error_file:
                writter = csv.writer(error_file)
                writter.writerow([code, station, component, type_i])
            return None
    return outter_etl_filter


#outter_tl_filter
def main():
    ##Script arguments
    script_path = os.getcwd()
    type_i = int(sys.argv[1])
    type_f = int(sys.argv[2])
    filter_args = get_filter_args()
    n_cpus = os.cpu_count()

    ##Database connection
    connection = sqlite3.connect(LOCAL_SEISMIC_DB)
    cursor = connection.cursor()

    ##Trace data extraction
    trace_df = extract_traces(cursor, 0, type_f)
    total_traces = len(trace_df)
    trace_df = trace_df.itertuples(name=None)

    ##Transform & load function
    etl_filter = fc_etl_filter(script_path, type_i, type_f, filter_args, total_traces)

    ##Transform & load function applied to trace data
    r = Parallel(n_jobs=n_cpus)(delayed(etl_filter)(trace_row) for trace_row in trace_df)
    r = list(filter(lambda t_r: t_r != None, r))

    ##Loading new trace to database
    if len(r) != 0:
        my_query = "INSERT INTO trace VALUES{}".format(str(tuple(r))[1:-1])
        cursor.execute(my_query)
        connection.commit()
        
    ##Closing database
    cursor.close()
    connection.close()


#Execution
if __name__ == "__main__":
    main()