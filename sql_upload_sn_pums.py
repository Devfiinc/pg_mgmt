import sys

import snsql
from snsql import Privacy
import pandas as pd

# Postgresql
import psycopg2
from config import config
from psycopg2.extensions import register_adapter, AsIs


def main(argv):
    csv_path = 'datasets/smartnoise-samples/data/readers/PUMS.csv'
    #meta_path = 'datasets/smartnoise-samples/data/readers/PUMS.yaml'

    data = pd.read_csv(csv_path)



    # Connect to Postgresql database
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()



    # Create table
    cur.execute("DROP TABLE IF EXISTS pums;"
                "CREATE TABLE IF NOT EXISTS pums ("
                    "pid        INT PRIMARY KEY NOT NULL,"
                    "age        INT,"
                    "sex        INT,"
                    "educ       INT,"
                    "race       INT,"
                    "income     INT,"
                    "married    INT"
                    ")")
    cur.execute("CREATE INDEX idx_pid ON pums(pid)")
    cur.execute("SET search_path = schema1, public;")


    print(data.head())


    for i in range(len(data)):
        cur.execute("INSERT INTO pums("
                        "pid,"
                        "age,"
                        "sex,"
                        "educ,"
                        "race,"
                        "income,"
                        "married"
                        ")"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        int(data.iloc[i]["pid"]),
                        int(data.iloc[i]["age"]),
                        int(data.iloc[i]["sex"]),
                        int(data.iloc[i]["educ"]),
                        int(data.iloc[i]["race"]),
                        int(data.iloc[i]["income"]),
                        int(data.iloc[i]["married"])
                    )
                    )           




    #cur.execute("SELECT pid age FROM pums")
    #aaa = cur.fetchall()
    #print(aaa)

    #for a in aaa:
    #    print(a, aaa[a])


    # Close connection
    cur.close()
    conn.close()






#    privacy = Privacy(epsilon=1.0, delta=0.01)
#    reader = snsql.from_connection(data, privacy=privacy, metadata=meta_path)
#
#    result = reader.execute('SELECT sex, AVG(age) AS age FROM PUMS.PUMS GROUP BY sex')
#
#    print(result)









if __name__ == '__main__':
    main(sys.argv)
