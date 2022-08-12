# Import OS and disable warnings for enhanced demo
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 

import sys
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import pickle

import tensorflow as tf
import tensorflow_federated as tff

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



def write_imagedata(imagedata, outputfile):
    print("  Write image data" + outputfile)
    global height_, width_
    header = np.array([0x0803, len(imagedata), height_, width_], dtype='>i4')
    with open(outputfile, "wb") as f:
        f.write(header.tobytes())
        f.write(imagedata.tobytes())

        

def write_labeldata(labeldata, outputfile):
    print("  Write label data" + outputfile)
    header = np.array([0x0801, len(labeldata)], dtype='>i4')
    with open(outputfile, "wb") as f:
        f.write(header.tobytes())
        f.write(labeldata.tobytes())



def load_dataset(dataset_name):
    if dataset_name == "celeba":
        return tff.simulation.datasets.celeba.load_data(
            split_by_clients=True, cache_dir=None
        )
    elif dataset_name == "cifar100":
        return tff.simulation.datasets.cifar100.load_data(
            cache_dir=None
        )
    elif dataset_name == "emnist":
        #return tff.simulation.datasets.emnist.load_data()
        return tff.simulation.datasets.emnist.load_data(
            only_digits=True, cache_dir=None
        )
    elif dataset_name == "gldv2":
        return tff.simulation.datasets.gldv2.load_data(
            1,                  #num_worker
            'cache',      #cache_dir
            False,       #gld23k
            GLD_SHARD_BASE_URL #base_url
        )
    #elif dataset_name == "inaturalist":
    #    return tff.simulation.datasets.inaturalist.load_data(
    #        'images', #image_dir
    #        'cache',  #cache_dir
    #        tff.simulation.datasets.inaturalist.INaturalistSplit.USER_120K #split
    #    ) -> Tuple[tff.simulation.datasets.ClientData, tf.data.Dataset]
    #elif dataset_name == "shakespeare":
    #    return tff.simulation.datasets.shakespeare.load_data(
    #        None #cache_dir
    #    ) -> Tuple[tff.simulation.datasets.ClientData, tff.simulation.datasets.ClientData]
    elif dataset_name == "stackoverflow":
        tff.simulation.datasets.stackoverflow.load_data(
            cache_dir=None
        )
    else:
        print("Dataset not found!")



def main(argv):
    # os.system('pg_ctlcluster 12 main start')
    #os.system('sudo pg_ctlcluster 14 main start')
    
    global height_, width_, channels_
    global sel_dataset_, num_clients_, client_idx_

    height_ = 28
    width_ = 28
    channels_ = 1
    
    #os.system('clear')

    # CMD input 
    if (len(argv) < 4):
        print()
        print("Wrong number of arguments. Expected input as follows:")
        print("  Dataset name (default: emnist)")
        print("  Number of clients (default: 2)")
        print("  Client id (default: 0)")
        print("Using default parameters")
        print()
        
        sel_dataset_ = "emnist"
        num_clients_ = 10
        client_idx_ = 0

    else:
        sel_dataset_ = argv[1]
        num_clients_ = int(argv[2])
        client_idx_ = int(argv[3])

    print("Federated dataset client", client_idx_)
    print("  Dataset:      ", sel_dataset_)
    print("  Total clients:", num_clients_)
    print()


    # Download eMNIST dataset from Tensorflow-Federated
    emnist_train, emnist_test = load_dataset(sel_dataset_)

    # Preprocess the data for feeding to Scikit
    client_data = make_dict(emnist_train, emnist_test, num_clients_)

    # Connect to Postgresql database
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()


    # Create table
    cur.execute("DROP TABLE IF EXISTS emnist;"
                "CREATE TABLE IF NOT EXISTS emnist ("
                    "id INT PRIMARY KEY NOT NULL,"
                    "client CHAR(8)," 
                    "image BYTEA,"
                    "imagea INTEGER[],"
                    "label INT"
                    ")")
    
    cur.execute("CREATE INDEX idx_client ON emnist(client)")
    
    cur.execute("SET search_path = schema1, public;")


    vId = 0
    cnt = 0
    for client in client_data:

        cnt += 1
        print("Uploading", cnt, "/", len(client_data))

        for i in range(len(client_data[client][0])):
            
            cur.execute("INSERT INTO emnist("
                            "id," 
                            "client,"
                            "image,"
                            "imagea,"
                            "label)"
                        "VALUES (%s, %s, %s, %s, %s)",
                        (vId, 
                         client, 
                         pickle.dumps(client_data[client][0][i]), 
                         client_data[client][0][i].astype(int).tolist(), 
                         client_data[client][1][i])
            )
            vId += 1
    
    # cur.execute("SELECT DISTINCT client imagea FROM emnist")
    # print(cur.fetchall())

    # Close connection
    cur.close()
    conn.close()


if __name__ == '__main__':
    main(sys.argv)
