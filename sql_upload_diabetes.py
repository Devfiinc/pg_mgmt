import sys

import pandas as pd

# Postgresql
import psycopg2
from config import config
from psycopg2.extensions import register_adapter, AsIs




def main(argv):
    csv_path = 'datasets/diabetes/diabetes.csv'

    data = pd.read_csv(csv_path)

    print(data.head())


    # Connect to Postgresql database
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()



    # Create table
    query  = "DROP TABLE IF EXISTS nki;"
    query += "CREATE TABLE IF NOT EXISTS nki ("


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
            query += d.lower() + " CHAR(" + str(len(d)) + ")"

        midval = True
        
        if ncol == 15:
            break

    query += ")"

    #print(query)

    # Create table
    cur.execute(query)
    cur.execute("CREATE INDEX idx_" + index + " ON nki(" + index + ")")
    cur.execute("SET search_path = schema1, public;")





    # Upload data
    for i in range(len(data)):

        print("Loading",i+1,"from",len(data))
        

        query  = "INSERT INTO nki("

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
                query += "'" + str(data.iloc[i][d]) + "'"
                    
            midval = True


        query += ")"

        #print(query)

        cur.execute(query)



    #cur.execute("SELECT pid age FROM nki")
    #aaa = cur.fetchall()
    #print(aaa)

    # Close connection
    cur.close()
    conn.close()

if __name__ == '__main__':
    main(sys.argv)
