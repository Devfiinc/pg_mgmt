# Import OS and disable warnings for enhanced demo
import os
#from re import X
#from tkinter import Y
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 

import sys
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import pickle

import tensorflow as tf
#import tensorflow_federated as tff


from matplotlib.cbook import to_filehandle
from sklearn.datasets import fetch_openml
from keras.utils.np_utils import to_categorical
import numpy as np
from sklearn.model_selection import train_test_split



# Postgresql
import psycopg2
from config import config
from psycopg2.extensions import register_adapter, AsIs


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
    
def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)
    
def addapt_numpy_int8(numpy_int8):
    return AsIs(numpy_int8)
    

def addapt_numpy_uint8(numpy_uint8):
    return AsIs(numpy_uint8)


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)


def addapt_numpy_array(numpy_array):
    return AsIs(tuple(numpy_array))






register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.int32, addapt_numpy_int32)
register_adapter(np.int8, addapt_numpy_int8)

register_adapter(np.uint8, addapt_numpy_uint8)

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.float32, addapt_numpy_float32)

register_adapter(np.ndarray, addapt_numpy_array)




def make_dict(train, test, num_export = 0):
    print("Make dict from TF Dataset")

    global height_, width_, channels_

    client_data = {}

    num_records = len(train.client_ids)

    if (num_export==0):
        num_export = len(train.client_ids)

    for i in range(num_export):
        client_id = train.client_ids[i]

        dataset_train = train.create_tf_dataset_for_client(train.client_ids[i])
        
        num_items = len(list(dataset_train))
        print("Num items in dataset", i, "/", num_export, "=", num_items)

        trainImageData = np.zeros(
            (num_items, height_ * width_), dtype=np.single)
        trainLabel = np.zeros(num_items, dtype=np.uint8)
        
        j = 0
        for data in dataset_train:
            label_i = data['label'].numpy()
            pixels_i = data['pixels'].numpy()

            pixels_i *= 255
            pixels_i = pixels_i.astype(int)
            pixels_i = pixels_i.flatten()

            trainLabel[j] = label_i
            trainImageData[j] = pixels_i
            j += 1

        dataset_test = test.create_tf_dataset_for_client(test.client_ids[i])

        testImageData = np.zeros(
            (num_items, height_ * width_), dtype=np.single)
        testLabel = np.zeros(num_items, dtype=np.uint8)
        
        j = 0
        for data in dataset_test:
            label_i = data['label'].numpy()
            pixels_i = data['pixels'].numpy()

            pixels_i *= 255
            pixels_i = pixels_i.astype(int)
            pixels_i = pixels_i.flatten()

            testLabel[j] = label_i
            testImageData[j] = pixels_i
            j += 1
        
        client_data[client_id] = [trainImageData, trainLabel, testImageData, testLabel]
        
    return client_data



        



def main(argv):
    # os.system('pg_ctlcluster 12 main start')
    #os.system('sudo pg_ctlcluster 14 main start')
    
    global height_, width_, channels_
    global sel_dataset_, num_clients_

    height_ = 28
    width_ = 28
    channels_ = 1
    
    sel_dataset_ = "emnist"
    num_clients_ = 10


    x, y = fetch_openml('mnist_784', version=1, return_X_y=True)
    #x = (x/255).astype('float32')
    #y = to_categorical(y)
    x = x.to_numpy()

    print(y[0])
    print(y[1])
    print(y[2])


    # Connect to Postgresql database
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()


    # Create table
    cur.execute("DROP TABLE IF EXISTS mnist;"
                "CREATE TABLE IF NOT EXISTS mnist ("
                    "id INT PRIMARY KEY NOT NULL,"
                    "image INT[],"
                    "label INT"
                    ")")
    
    cur.execute("SET search_path = schema1, public;")


    vId = 0
    for xi,yi in zip(x, y):

        print("Uploading", vId+1, "/", len(x))
            
        cur.execute("INSERT INTO mnist("
                        "id," 
                        "image,"
                        "label)"
                    "VALUES (%s, %s, %s)",
                    (vId, 
                     xi.tolist(), 
                     yi)
        )
        vId += 1
    

    # Close connection
    cur.close()
    conn.close()


if __name__ == '__main__':
    main(sys.argv)
