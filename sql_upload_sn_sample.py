import sys
import warnings

import pandas as pd
import numpy as np

from sklearn.datasets import make_classification
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score

import matplotlib.pyplot as plt

import seaborn as sns

# Postgresql
import psycopg2
from config import config
from psycopg2.extensions import register_adapter, AsIs


# define feature names
def create_feature_names (n_items, prefix = 'feature'):
    names = []
    for i in range(n_items):
        names.append(prefix + '_' + str(i))
    return names


def main(argv):
    #100,000 instances, 10 classes
    #20 ordinal features: mixture of relevant ('informative'), redundant, repeated and pure noise
    #3% of instances are mislabeled

    n_samples = 100000
    n_features = 20
    n_informative = 7 # relevant features to explain target
    n_redundant = 5 # linear combinations of informative
    n_repeated = 5 # random copies of informative and redundant
    n_useless = n_features - n_informative - n_redundant - n_repeated # noise

    n_classes = 10
    seed = 1


    inf_features = create_feature_names(n_informative, 'inf')
    red_features = create_feature_names(n_redundant, 'red')
    rep_features = create_feature_names(n_repeated, 'rep')
    useless_features = create_feature_names(n_useless, 'noise')

    feature_names = inf_features + red_features + rep_features + useless_features

    X, y = make_classification(n_samples=n_samples, 
                        n_features=n_features, 
                        n_informative=n_informative, 
                        n_redundant=n_redundant, 
                        n_repeated=n_repeated, 
                        n_classes=n_classes, 
                        n_clusters_per_class=2, 
                        weights=None, 
                        flip_y=0.03, 
                        class_sep=5.0, 
                        hypercube=True, 
                        shift=15.0,
                        scale=0.5,
                        shuffle=False, 
                        random_state=seed)

    # Convert to Dataframe
    Z=np.zeros((X.shape[0], X.shape[1]+1))
    Z[:,:-1]=X
    Z[:,-1]=y

    columns = feature_names + ['class']

    df = pd.DataFrame(Z, columns=columns)
    df['class'] = df['class'].astype('int32')

    print(df.head(8))

    # Connect to Postgresql database
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()

    # Create table
    query  = "DROP TABLE IF EXISTS sn;"
    query += "CREATE TABLE IF NOT EXISTS sn ("

    ncol = 0
    midval = False
    index = ""
    headers = []
    for d in df:

        headers.append(d)
        ncol += 1

        if midval:
            query += ", "
        else:
            index = d

        dtype = pd.api.types.infer_dtype([df.iloc[0][d]])

        if dtype == "integer":
            query += d.lower() + " INT"
        elif dtype == "floating":
            query += d.lower() + " FLOAT"
        else:
            query += d.lower() + " CHAR(" + str(len(d)) + ")"

        midval = True
        
        if ncol == 22:
            break

    query += ")"

    # Create table
    cur.execute(query)
    cur.execute("CREATE INDEX idx_" + index + " ON sn(" + index + ")")
    cur.execute("SET search_path = schema1, public;")








    # Upload data
    for i in range(len(df)):

        print("Loading", i, "of", range(len(df)))

        query  = "INSERT INTO sn("

        midval = False
        for d in headers:
            if midval:
                query += ", "
            query += d.lower()
            midval = True

        query += ")"
        query += " VALUES ("


        

        midval = False
        for d in headers:
            if midval:
                query += ", "

            dtype = pd.api.types.infer_dtype([df.iloc[i][d]])

            if dtype == "integer":
                query += str(int(df.iloc[i][d]))
            elif dtype == "floating":
                query += str(df.iloc[i][d])
            else:
                query += "'" + str(df.iloc[i][d]) + "'"
                    
            midval = True

        query += ")"

        cur.execute(query)





    # Close connection
    cur.close()
    conn.close()

if __name__ == '__main__':
    main(sys.argv)
