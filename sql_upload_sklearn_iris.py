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


onehot_encoder = OneHotEncoder(sparse=False)


import matplotlib.pyplot as plt

import seaborn as sns

# Postgresql
import psycopg2
from config import config
from psycopg2.extensions import register_adapter, AsIs



def main(argv):
    #100,000 instances, 10 classes
    #20 ordinal features: mixture of relevant ('informative'), redundant, repeated and pure noise
    #3% of instances are mislabeled

    X = load_iris().data
    Y = load_iris().target




    # Connect to Postgresql database
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()

    # Create table
    query  = "DROP TABLE IF EXISTS iris;"
    query += "CREATE TABLE IF NOT EXISTS iris ("
    #query += "id INT PRIMARY KEY NOT NULL,"
    query += "v1 FLOAT,"
    query += "v2 FLOAT,"
    query += "v3 FLOAT,"
    query += "v4 FLOAT,"
    query += "y FLOAT"
    query += ")"

    # Create table
    cur.execute(query)
    #cur.execute("CREATE INDEX idx_id ON iris(id)")
    cur.execute("SET search_path = schema1, public;")


    for i in range(len(X)):
        query  = "INSERT INTO iris("
        #query += "id,"
        query += "v1,"
        query += "v2,"
        query += "v3,"
        query += "v4,"
        query += "y) "
        query += "VALUES ("
        #query += str(i) + ", "
        query += str(X[i][0]) + ", "
        query += str(X[i][1]) + ", "
        query += str(X[i][2]) + ", "
        query += str(X[i][3]) + ", "
        query += str(float(Y[i]))
        query += ")"

        cur.execute(query)




    # Close connection
    cur.close()
    conn.close()

if __name__ == '__main__':
    main(sys.argv)
