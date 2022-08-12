import sys
import warnings

import pandas as pd
import numpy as np

from sklearn.datasets import make_classification
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from scipy.special import softmax
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score

from sklearn.datasets import load_iris

from os import listdir
from PIL import Image as PImage


onehot_encoder = OneHotEncoder(sparse=False)


import matplotlib.pyplot as plt

import seaborn as sns

# Postgresql
import psycopg2
from config import config
from psycopg2.extensions import register_adapter, AsIs

import cv2



class imdata:
    def __init__(self, id, im, imx, imy):
        self._id = id
        self._im = im
        self._imx = imx.flatten() #.flatten().astype(float) / 255.0
        self._imy = imy.flatten() #.flatten().astype(float) / 255.0
        self._dim = imx.shape

    def printx(self):
        cv2.imshow("x(" + str(self._id) + "-" + str(self._im) + ")", self._imx.reshape(self._dim))
        cv2.waitKey(0)

    def printy(self):
        cv2.imshow("y(" + str(self._id) + "-" + str(self._im) + ")", self._imy.reshape(self._dim))
        cv2.waitKey(0)





def load_images(path, train):
    image_list = listdir(path)
    image_list = sorted(image_list)
    imx = []
    imy = []

    data = []
    files = []

    for image in image_list:
        if "mask" in image:
            continue
        else:
            files.append(image[:-4])

    cnt = 0
    for file in files:
        cnt = cnt + 1
        print("Loading image:", cnt, "of", len(files))
        if train:
            id, im = file.split("_", 1)
            imx = cv2.imread(path + file + ".tif", cv2.IMREAD_GRAYSCALE)
            imy = cv2.imread(path + file + "_mask.tif", cv2.IMREAD_GRAYSCALE)
            data.append(imdata(id, im, imx, imy))
        else:
            id = file
            im = "0"
            imx = cv2.imread(path + file + ".tif", cv2.IMREAD_GRAYSCALE)
            imy = np.array([0])
            data.append(imdata(id, im, imx, imy))

    return data


def list_files(path):
    image_list = listdir(path)
    image_list = sorted(image_list)
    files = []

    for image in image_list:
        if "mask" in image:
            continue
        else:
            files.append(image[:-4])

    return files



def load_image(path, file, train, dim = (0,0)):
    if train:
        id, im = file.split("_", 1)
        imx = cv2.imread(path + file + ".tif", cv2.IMREAD_GRAYSCALE)
        imy = cv2.imread(path + file + "_mask.tif", cv2.IMREAD_GRAYSCALE)

        if dim[0] > 0:
            imx = cv2.resize(imx, dim, interpolation = cv2.INTER_AREA)
            imy = cv2.resize(imy, dim, interpolation = cv2.INTER_AREA)

        return imdata(id, im, imx, imy)
    else:
        imx = cv2.imread(path + file + ".tif", cv2.IMREAD_GRAYSCALE)

        if dim[0] > 0:
            imx = cv2.resize(imx, dim, interpolation = cv2.INTER_AREA)

        return imdata(file, "0", imx, np.array([0]))



def main(argv):

    # Connect to Postgresql database
    print("Connect to database")
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()

    # Create table
    print("Create table")
    query  = "DROP TABLE IF EXISTS nerves;"
    query += "CREATE TABLE IF NOT EXISTS nerves ("
    query += "  patient_id    INT,"
    query += "  image_id      INT,"
    query += "  imx           INT[],"
    query += "  imy           INT[]"
    query += ")"
    cur.execute(query)

    # Create index
    cur.execute("CREATE INDEX patient_idx ON nerves(patient_id)")

    cur.execute("SET search_path = schema1, public;")


    path_train = "/home/icirauqui/w0rkspace_Arc/sn/datasets/ultrasound-nerve-segmentation/train/"
    files_train = list_files(path_train)

    dim = (96, 96)
    #dim = (24, 24)

    train_cnt = 1
    for file in files_train:
        print("Loading train:", train_cnt, "of", len(files_train))
        train_cnt += 1

        imdata = load_image(path_train, file, True, dim)

        query  = "INSERT INTO nerves("
        query += "patient_id,"
        query += "image_id  ,"
        query += "imx       ,"
        query += "imy       "
        query += ") VALUES ("
        query += str(imdata._id) + ", "
        query += str(imdata._im) + ", "
        query += "ARRAY" + str((imdata._imx).tolist()) + ", "
        query += "ARRAY" + str((imdata._imy).tolist())
        query += ")"
        cur.execute(query)



    path_test = "/home/icirauqui/w0rkspace_Arc/sn/datasets/ultrasound-nerve-segmentation/test/"
    files_test = list_files(path_test)

    test_cnt = 1
    for file in files_test:
        print("Loading test:", test_cnt, "of", len(files_test))
        test_cnt += 1

        imdata = load_image(path_test, file, False, dim)

        query  = "INSERT INTO nerves("
        query += "patient_id,"
        query += "image_id  ,"
        query += "imx       "
        query += ") VALUES ("
        query += str(10000 + int(imdata._id)) + ", "
        query += str(imdata._im) + ", "
        query += "ARRAY" + str((imdata._imx).tolist())
        query += ")"
        cur.execute(query)



    # Close connection
    cur.close()
    conn.close()


"""
    # Connect to Postgresql database
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT imx FROM nerves;")
    res1 = cur.fetchall()

    for r in res1:
        print(r)

    # Close connection
    cur.close()
    conn.close()
"""


if __name__ == '__main__':
    main(sys.argv)
