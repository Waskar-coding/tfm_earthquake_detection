#Description
"""
Calculates spectrums from traces and saves them as images.
To execute this script you need to activate a conda environment with obpsy installed:
conda activate [obspy environment]
These are the arguments to execute the script:
python etl_traces_spectrum.py type_i type_f
    type_i: Initial type, an integer that references a trace type
    type_f: Target type, an integer that references a spectrum type
Both types must be indexes in TRACE_LOCATION_MAP from load_db
"""

#Tags
__author__ = "Óscar Gómez Nonnato"
__date__ = "05/09/2021"

#Libraries
##Standard
import os
from sqlite3.dbapi2 import connect
import sys

##Packages
from joblib import Parallel, delayed
import matplotlib.pyplot as plt
import obspy
import pandas as pd
import sqlite3

##Local
from extract_traces import extract_traces
from load_db import LOCAL_SEISMIC_DB, TRACE_LOCATION_MAP

#Constants
SAMPLING_RATE = 100


#Creating spectrogram from trace
def inner_etl_spectrum(trace_name, type_i, type_f):
    ##Documentation
    """
    Description
    -----------
    Opens a trace using obspy, modifies the sampling rate to the constant SAMPLING_RATE
    and extracts the signal data which is used to calculate as spectrogram. The axis of
    the spectrogram is removed and the image is saved to a new direction, its file name
    is returned.
    
    Parameters
    ----------
    trace_name: str
        Name of the trace with which the spectrogram is calculated
    type_i: int
        Trace initial type
    type_f: int
        Spectrogram final type
    
    Returns
    -------
    new_trace_location: str
        Location of the spectrogram image
    """
    
    ##Extracting trace data
    os.chdir(TRACE_LOCATION_MAP[type_i])
    tr = obspy.read(trace_name)
    tr.resample(SAMPLING_RATE)
    tr_data = tr[0].data

    ##Calculating spectrogram
    fig, ax = plt.subplots(1)
    ax.specgram(
        x = tr_data,
        Fs=SAMPLING_RATE,
        cmap='gray',
        NFFT=256
        )

    ##Removing axis from image
    ax.axis('off')
    plt.gca().set_axis_off()
    plt.subplots_adjust(left=0,right=1,bottom=0,top=1,hspace=0,wspace=0)
    plt.margins(0,0)

    ##Saving image and returning its name
    os.chdir(TRACE_LOCATION_MAP[type_f])
    new_trace_name = '_'.join(trace_name.split('_')[:-1]) + '_' + str(type_f) + '.png'
    new_trace_location = os.path.join(TRACE_LOCATION_MAP[type_f], new_trace_name)
    fig.savefig(new_trace_name)
    plt.close()
    return new_trace_location


#Creating etl_spectrum wrapper
def fo_etl_spectrum(type_i, type_f, total_traces):
    ##Documentation
    """
    Description
    -----------
    First order function, creates a wrapper for inner_etl_spectrum using the
    arguments passed to the scripts and the total number of trace registers found
    
    Parameters
    ----------
    type_i: int
        Trace initial type
    type_f: int
        Spectrogram final type
    total_traces: int
        Total number of traces found
    
    Returns
    -------
    outter_etl_spectrum: function
        Wrapper for inner_etl_spectrum
    """
    ##Creating a wrapper function
    def outter_etl_spectrum(trace):
        i, code, station, component, start, final, file = trace
        new_trace_name = inner_etl_spectrum(file, type_i, type_f)
        print('{}_{}_{}: {}/{}'.format(code, station, component, i+1, total_traces))
        return (code, station, component, start, final, type_f, new_trace_name)
    return outter_etl_spectrum


#Main
def main():
    ##Script arguments
    type_i = int(sys.argv[1])
    type_f = int(sys.argv[2])
    n_cpus = os.cpu_count()

    ##Database connection
    connection = sqlite3.connect(LOCAL_SEISMIC_DB)
    cursor = connection.cursor()

    ##Trace data extraction
    trace_df = extract_traces(cursor, type_i, type_f)
    total_traces = len(trace_df)
    trace_df = trace_df.itertuples(name=None)

    ##Transform & load function
    etl_spectrum = fo_etl_spectrum(type_i, type_f, total_traces)

    ##Transform & load function applied to trace data
    r = Parallel(n_jobs=n_cpus)(delayed(etl_spectrum)(trace_row) for trace_row in trace_df)

    ##Loading new trace data to database
    my_query = "INSERT INTO trace VALUES{}".format(str(tuple(r))[1:-1])
    cursor.execute(my_query)
    connection.commit()

    ##Closing database
    cursor.close()
    connection.close()


#Execution
if __name__ == '__main__':
    main()