import sys

import snsql
from snsql import Privacy
import pandas as pd

# Postgresql
import psycopg2
from config import config
from psycopg2.extensions import register_adapter, AsIs

import unicodedata


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')



def upload(table_name, data, col_limit=None):

    # Connect to Postgresql database
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()


    # Create table
    query  = "DROP TABLE IF EXISTS " + table_name + "; "
    query += "CREATE TABLE IF NOT EXISTS " + table_name + " ("

    ncol = 0
    midval = False
    index = ""
    headers = []
    for d in data:
        headers.append(d)
        ncol += 1

        if midval:
            query += ", "
        else:
            index = d

        dtype = pd.api.types.infer_dtype([data.iloc[0][d]])

        if dtype == "integer":
            query += d.lower() + " INT"
        elif dtype == "floating":
            query += d.lower() + " FLOAT"
        else:
            dim_col = max(data[d].astype(str).str.len())
            query += d.lower() + " CHAR(" + str(dim_col) + ")"

        midval = True
        
        if ncol == col_limit:
            break

    query += ")"

    print(query)

    

    # Create table
    cur.execute(query)
    cur.execute("CREATE INDEX idx_" + index + " ON " + table_name + "(" + index + ")")
    cur.execute("SET search_path = schema1, public;")




    # Upload data
    for i in range(len(data)):

        print("Loading",i+1,"from",len(data))
        

        query  = "INSERT INTO " + table_name + "("

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

            dtype = pd.api.types.infer_dtype([data.iloc[i][d]])

            #if dtype == "string":
            #    query += "'" + str(data.iloc[i][d]) + "'"
            #else:
            #    query += str(data.iloc[i][d])

            if dtype == "integer":
                query += str(int(data.iloc[i][d]))
            elif dtype == "floating":
                query += str(data.iloc[i][d])
            else:
                #query += "$$" + strip_accents(str(data.iloc[i][d])) + "$$"
                query += "$$" + str(data.iloc[i][d]) + "$$"
                    
            midval = True


        query += ")"

        #print(query)

        cur.execute(query)


    # Close connection
    cur.close()
    conn.close()







def main(argv):
    csv_path_bank = 'datasets/PETs/bank_swift/bank_dataset/bank_dataset.csv'
    csv_path_swift_train = 'datasets/PETs/bank_swift/swift_transaction_train_dataset/swift_transaction_train_dataset.csv'
    csv_path_swift_test = 'datasets/PETs/bank_swift/bank_dataset/bank_dataset.csv'

    data_bank = pd.read_csv(csv_path_bank)
    print(data_bank.head())
    upload("bank", data_bank)

    data_swift_train = pd.read_csv(csv_path_swift_train)
    print(data_swift_train.head())
    upload("swift_train", data_swift_train)

    data_swift_test  = pd.read_csv(csv_path_swift_test)
    print(data_swift_test.head())
    upload("swift_test", data_swift_test)



if __name__ == '__main__':
    main(sys.argv)
