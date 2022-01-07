#Description
"""
Generates tensorflow tranning and test records from spectrogram images to
train object detection algorithms
"""

#Tags
__author__ = "Óscar Gómez Nonnato"
__date__ = "08/09/2021"


#Libraries
##Standard
from datetime import datetime
from math import ceil, floor
import os
from random import choice, randint
import sys

##Packages
import matplotlib.pyplot as plt
from object_detection.utils import dataset_util
import pandas as pd
import sqlite3
import tensorflow.compat.v1 as tf

##Local
from load_db import LOCAL_SEISMIC_DB, TRACE_LOCATION_MAP

##Constants
P_MARGIN = 100
S_MARGIN = 5000
IMAGE_FORMAT = b'png'
TRAIN_TEST_SPLIT = 0.9


#Extract traces and registers information
def extract_spectrograms(spectrogram_type):
    ##Documentation
    """
    Description
    -----------
    Extracts spectrogram image and register data from the local database
    
    Parameters
    ----------
    spectrogram_type: int
        Type of spectrogram images
    
    Returns
    -------
    spectrograms: pandas.core.frame.DataFrame
        Dataframe with spectrogram image and register data
    """
    
    ##Connecting to db
    connection = sqlite3.Connection(LOCAL_SEISMIC_DB)
    cursor = connection.cursor()

    ##Querying db and creating dataframe
    my_query = """select t.code, t.name, t.component, t.start, t.final, r.p_time, r.s_time, t.location\
        from trace as t join register as r on t.code=r.code and t.name=r.name\
            where t.type={}""".format(spectrogram_type)
    spectrograms = pd.DataFrame(cursor.execute(my_query))
    spectrograms.columns = ('code', 'name', 'component', 'start', 'final', 'p_pick', 's_pick', 'location')

    ##Closing db
    cursor.close()
    connection.close()
    return spectrograms


#Exctracting dimensions from an example image
def extract_image_size():
    ##Documentation
    """
    Description
    -----------
    Reads a random image from the folder and extracts its size.
    All spectrogram image are supposed to have the same dimensions.
    
    Returns
    -------
    shape: tuple
        Image dimensions (heigth, width, channel)
    """
    
    ##Loading random image and returning dimensions
    file = choice(os.listdir())
    fig = plt.imread(file)
    shape = fig.shape
    return shape
    

#Transforming spectrogram dataframe with image information
def transform_spectrograms(spectrograms, img_window):
    ##Documentation
    """
    Description
    -----------
    Locates the P and S onsets into the spectrogram's image. Both onset times
    are converted into pixel coordinates within the image, the window between
    the two onsets is expanded a backwards and forwards according to the P_MARGIN
    and the S_MARGIN. The limits of the new window are selected as the start and
    the final of the earthquake signal.

    Parameters
    ----------
    spectrograms: pandas.core.frame.DataFrame
        Dataframe with spectrogram image and register data
    img_window: int
        Spectrogram images width in pixels
    
    Returns
    -------
    spectrograms: pandas.core.frame.DataFrame
        A modified spectrogram dataframe, includes P and S onsets as well as
        earthquake signal start and end coordinates in pixel units.
    """
    
    ##Creating file column from locations
    spectrograms['file'] = [ trace_location.split('\\')[-1] for trace_location in spectrograms['location']]
    del spectrograms['location']

    ##Converting timestamps to datetime objects
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
    TIME_FORMAT = "%H:%M:%S.%f"
    to_datetime = lambda d,f: [ datetime.strptime(x,f) for x in spectrograms[d] ]
    spectrograms['start'] = to_datetime('start', DATETIME_FORMAT)
    spectrograms['final'] = to_datetime('final', DATETIME_FORMAT)
    spectrograms['p_pick'] = to_datetime('p_pick', TIME_FORMAT)
    spectrograms['s_pick'] = to_datetime('s_pick', TIME_FORMAT)

    ##Asigning p_pick and s_pick a date
    assign_date = lambda c: [datetime.combine(d.date(),t.time()) for d,t in zip(spectrograms['start'],spectrograms[c])]
    spectrograms['p_pick'] = assign_date('p_pick')
    spectrograms['s_pick'] = assign_date('s_pick')

    ##Converting time intervals from start to total miliseconds
    to_interval = lambda d: [ pick.total_seconds() * 1000 for pick in (spectrograms[d] - spectrograms['start'])]
    spectrograms['window'] = to_interval('final')
    spectrograms['p_pick'] = to_interval('p_pick')
    spectrograms['s_pick'] = to_interval('s_pick')
    del spectrograms['start']
    del spectrograms['final']

    ##Converting time interval into image pixels
    to_image_coords = lambda p: [ floor(pick) for pick in spectrograms[p]*spectrograms['ppms'] ]
    spectrograms['ppms'] = img_window/spectrograms['window']
    spectrograms['p_pick'] = to_image_coords('p_pick')
    spectrograms['s_pick'] = to_image_coords('s_pick')
    del spectrograms['window']

    ##Using margins to select earthquake
    ajust_limit = lambda e_f: e_f if e_f<=img_window else img_window
    spectrograms['e_start'] = spectrograms['p_pick'] - [ceil(value) for value in P_MARGIN*spectrograms['ppms']]
    spectrograms['e_final'] = spectrograms['s_pick'] + [ceil(value) for value in S_MARGIN*spectrograms['ppms']]
    print(img_window)
    print(spectrograms)
    spectrograms['e_final'] = [ajust_limit(e_f) for e_f in spectrograms['e_final']]
    del spectrograms['ppms']

    return spectrograms


##Visualizing e_start and e_final on the spectrograms
def mark_one(spectrograms):
    ##Documentation
    """
    Description
    -----------
    Pics a random spectrogram image and plots the earthquake's signal 
    start and final marks.
    
    Parameters
    ----------
    spectrograms: pandas.core.frame.DataFrame
        A modified spectrogram dataframe, includes P and S onsets as well as
        earthquake signal start and end coordinates in pixel units.
    """
    
    ##Selecting a random spectrogram
    r = randint(0, len(spectrograms)-1)
    example = spectrograms.iloc[r]

    ##Showing raw image
    image = plt.imread(example['file'])
    plt.imshow(image)
    plt.show()

    ##Boxing earthquake and showing again
    plt.axvline(example['e_start'])
    plt.axvline(example['e_final'])
    plt.imshow(image)
    plt.show()


#Create record
def create_record(path ,filename, e_start, e_final, width, height):
    ##Documentation
    """
    Description
    -----------
    Creates a tensorflow record example to train an object 
    detection model.
    
    Parameters
    ----------
    path: str
        Image folder path
    filename: str
        Image file name
    e_start: int
        Earthquake signal start in pixel coordinates
    e_final: int
        Earthquake signal final in pixel coordinates
    width: int
        Image width
    height: int
        Image height
    
    Returns
    -------
    tf_example: tf.train.Example
        Tensorflow record used to train an object detection model
    """
    
    ##Loading image
    with tf.gfile.GFile(os.path.join(path, filename), 'rb') as fid:
        encoded_jpg = fid.read()
    
    ##Creating record parameters
    filename = filename.encode('utf8')
    xmins = [e_start/width]
    xmaxs = [e_final/width]
    ymins = [0]
    ymaxs = [1]
    classes_text = ['earthquake'.encode('utf8')]
    classes = [1]

    ##Creating example
    tf_example = tf.train.Example(features=tf.train.Features(feature={
        'image/height': dataset_util.int64_feature(height),
        'image/width': dataset_util.int64_feature(width),
        'image/filename': dataset_util.bytes_feature(filename),
        'image/source_id': dataset_util.bytes_feature(filename),
        'image/encoded': dataset_util.bytes_feature(encoded_jpg),
        'image/format': dataset_util.bytes_feature(IMAGE_FORMAT),
        'image/object/bbox/xmin': dataset_util.float_list_feature(xmins),
        'image/object/bbox/xmax': dataset_util.float_list_feature(xmaxs),
        'image/object/bbox/ymin': dataset_util.float_list_feature(ymins),
        'image/object/bbox/ymax': dataset_util.float_list_feature(ymaxs),
        'image/object/class/text': dataset_util.bytes_list_feature(classes_text),
        'image/object/class/label': dataset_util.int64_list_feature(classes),
    }))
    return tf_example


#Loading record to file
def fc_load_record_file(spectrogram_folder, record_folder, width, height, total_spectrograms):
    ##Documentation
    """
    Description
    -----------
    Wrapper, that passes the script parameters to a function
    that loads examples to a record file.
    
    Parameters
    ----------
    spectrogram_folder: str
        Spectrogram images folder
    record_folder:
        Train and test record's folder
    width: int
        Image witdth
    height: int
        Image heigth
    total_spectrogram: int
        Total number of spectrogram images

    Returns
    -------
    load_record_file: function
        Function used to read spectrogram images and write new record examples
    """
    
    ##Using parameters to create the loading function
    def load_record_file(writer, s_i):
        i, s_i = s_i
        code = s_i['code']
        name = s_i['name']
        component = s_i['component']
        file = s_i['file']
        e_start = s_i['e_start']
        e_final = s_i['e_final']
        os.chdir(spectrogram_folder)
        tf_example = create_record(spectrogram_folder, file, e_start, e_final, width, height)
        os.chdir(record_folder)
        writer.write(tf_example.SerializeToString())
        print("{}_{}_{}: {}/{}".format(code, name, component, i+1, total_spectrograms))
    return load_record_file


#Load record to database
def load_record_db(spectrograms):
    ##Documentation
    """
    Description
    -----------
    Loads all spectrograms registered in the record file to the
    local database as a new type, all these new register's location
    is assigned to the train or the test record file.
    
    Parameters
    ----------
    spectrograms: pandas.core.frame.DataFrame
        Spectrograms train or test dataframe
    """
    
    ##Connecting to db
    connection = sqlite3.connect(LOCAL_SEISMIC_DB)
    cursor = connection.cursor()

    ##Inserting values into db
    spectrograms = str(list(spectrograms.itertuples(index=False, name=None)))[1:-1]
    my_query = "INSERT INTO record VALUES {}".format(spectrograms)
    cursor.execute(my_query)
    connection.commit()

    ##Closing db
    cursor.close()
    connection.close()


#Main
def main():
    ##Initial variables
    spectrogram_type = int(sys.argv[1])
    record_type = int(sys.argv[2])    
    train_record_name = sys.argv[3]
    test_record_name = sys.argv[4]
    script_folder = os.getcwd()
    spectrogram_folder = TRACE_LOCATION_MAP[spectrogram_type]
    record_folder = TRACE_LOCATION_MAP[record_type]
    train_record_location = os.path.join(record_folder, train_record_name)
    test_record_location = os.path.join(record_folder, test_record_name)

    ##Extracting spectrogram data from db
    spectrograms = extract_spectrograms(spectrogram_type)

    ##Extracting image dimensions
    os.chdir(spectrogram_folder)
    height, width, ch = extract_image_size()

    ##Transforming spectrogram time into image pixels
    spectrograms = transform_spectrograms(spectrograms, width)
    spectrograms['type'] = record_type
    cols = spectrograms.columns.tolist()
    cols = cols[:3] + [cols[8]] + cols[3:5] + cols[6:8] + [cols[5]]
    spectrograms = spectrograms[cols]
    total_spectrograms = len(spectrograms)
    split_index = int( TRAIN_TEST_SPLIT * total_spectrograms )

    ##Spliting spectrograms dataframe
    spectrograms = spectrograms.sample(frac=1).reset_index(drop=True)
    s_train = spectrograms.iloc[:split_index].copy()
    s_test = spectrograms.iloc[split_index:].copy()

    ##Creating a function to load the records
    load_record_file = fc_load_record_file(spectrogram_folder, record_folder, width, height, total_spectrograms)
    
    ##Writing train records
    writer_train = tf.python_io.TFRecordWriter(train_record_location)
    [load_record_file(writer_train, s_i) for s_i in s_train.iterrows()]
    writer_train.close()

    ##Writing test records
    writer_test = tf.python_io.TFRecordWriter(test_record_location)
    [load_record_file(writer_test, s_i) for s_i in s_test.iterrows()]
    writer_test.close()

    ##Loading records to db
    os.chdir(script_folder)
    del s_train['file']
    s_train['split'] = 0
    s_train['location'] = train_record_location
    del s_test['file']
    s_test['split'] = 1
    s_test['location'] = test_record_location
    load_record_db(s_train)
    load_record_db(s_test)


#Execution
if __name__ == "__main__":
    main()