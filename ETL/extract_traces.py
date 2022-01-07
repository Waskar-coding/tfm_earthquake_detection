#Description
"""
Useful functions to extract
traces from the local database
"""

#Tags
__author__ = "Óscar Gómez Nonnato"
__date__ = "06/09/2021"

#Libraries
##Packages
import pandas as pd


#Extract traces
def extract_traces(cursor, type_i, type_f):
    ##Documentation
    """
    Description
    -----------
    Extract all trace registers of type_i that do not have
    a type_f for the same code, station name and component.
    
    Parameters
    ----------
    cursor: sqlite3.Cursor
        sqlite3 cursor of the local database connection
    type_i: int
        Trace initial type
    type_f: int
        Trace final type
    
    Returns
    -------
    valid_traces: pandas.core.frame.DataFrame
        Dataframe containing the selected traces
    """
    
    ##Querying database
    traces_query = f"""
        select t1.code,t1.name,t1.component,t1.start,t1.final,t1.location from trace as t1 where t1.type={type_i} and not exists (\
            select t2.code from trace as t2 where t2.type={type_f} and t1.code=t2.code and t1.name=t2.name and t1.component=t2.component\
        )
    """
    valid_traces = cursor.execute(traces_query)

    ##Transforming response into dataframe
    valid_traces = pd.DataFrame(valid_traces)
    valid_traces.columns = ('code', 'name', 'component', 'start', 'final', 'location')

    ##Creating file column from locations
    valid_traces['file'] = [ trace_location.split('\\')[-1] for trace_location in valid_traces['location']]
    del valid_traces['location']
    return valid_traces