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



def upload(table_name, data, data_proc, data_label=None):

    # Connect to Postgresql database
    params = config()
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()

    headers = []
    for d in data:
        headers.append(d)
    for d in data_proc:
        headers.append(d)
        

    # Create table
    if table_name == "bank":
        query = "DROP TABLE IF EXISTS bank; "
        query += "CREATE TABLE IF NOT EXISTS bank ("
        query += "bank CHAR(8), "
        query += "account CHAR(30), "
        query += "name CHAR(80), "
        query += "street CHAR(80), "
        query += "countrycityzip CHAR(80), "
        query += "flags INT, "
        query += "flag INT);"


    elif table_name == "swift_train" or table_name == "swift_test":
        query = "DROP TABLE IF EXISTS " + table_name + "; "
        query += "CREATE TABLE IF NOT EXISTS " + table_name + " ("
        query += "MessageId CHAR(30), "
        query += "Timestamp CHAR(30), "
        query += "UETR CHAR(60), "
        query += "Sender CHAR(8), "
        query += "Receiver CHAR(8), "
        query += "TransactionReference CHAR(30), "
        query += "OrderingAccount CHAR(40), "
        query += "OrderingName CHAR(80), "
        query += "OrderingStreet CHAR(80), "
        query += "OrderingCountryCityZip CHAR(80), "
        query += "BeneficiaryAccount CHAR(40), "
        query += "BeneficiaryName CHAR(80), "
        query += "BeneficiaryStreet CHAR(80), "
        query += "BeneficiaryCountryCityZip CHAR(80), "
        query += "SettlementDate DATE, "
        query += "SettlementCurrency CHAR(3), "
        query += "SettlementAmount FLOAT, "
        query += "InstructedCurrency CHAR(3), "
        query += "InstructedAmount FLOAT, "
        query += "l1 FLOAT, "
        query += "l2 FLOAT, "
        query += "l3 FLOAT, "
        query += "l4 FLOAT, "
        query += "l5 FLOAT, "
        query += "l6 FLOAT, "
        query += "l7 FLOAT, "
        query += "Label INT);"






    else:
        query  = "DROP TABLE IF EXISTS " + table_name + "; "
        query += "CREATE TABLE IF NOT EXISTS " + table_name + " ("

        midval = False
        for d in data:

            if midval:
                query += ", "

            dtype = pd.api.types.infer_dtype([data.iloc[1][d]])

            if dtype == "integer":
                query += d.lower() + " INT"
            elif dtype == "floating":
                query += d.lower() + " FLOAT"
            else:
                dim_col = max(data[d].astype(str).str.len())
                query += d.lower() + " CHAR(" + str(dim_col) + ")"

            midval = True

        for d in data_proc:
            if midval:
                query += ", "

            dtype = pd.api.types.infer_dtype([data_proc.iloc[1][d]])

            if dtype == "integer":
                query += d.lower() + " INT"
            elif dtype == "floating":
                query += d.lower() + " FLOAT"
            else:
                dim_col = max(data[d].astype(str).str.len())
                query += d.lower() + " CHAR(" + str(dim_col) + ")"

        query += ")"

    # Create table
    cur.execute(query)
    cur.execute("SET search_path = schema1, public;")




    # Upload data

    if table_name == "bank":
    

        for i in range(len(data)):
            print("Loading",i+1,"from",len(data))

            query = "INSERT INTO bank ("
            query += "bank, "
            query += "account, "
            query += "name, "
            query += "street, "
            query += "countrycityzip, "
            query += "flags, "
            query += "flag) VALUES ("

            query += "$$" + str(data.iloc[i]["Bank"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["Account"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["Name"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["Street"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["CountryCityZip"]) + "$$" + ", "
            query += str(int(data.iloc[i]["Flags"])) + ", "
            query += str(int(data_proc.iloc[i]["Flag"]))
            query += ");"

            cur.execute(query)

    elif table_name == "swift_train" or table_name == "swift_test":

        for i in range(len(data)):
            print("Loading",i+1,"from",len(data))

            query = "INSERT INTO " + table_name + " ("
            query += "MessageId, "
            query += "Timestamp, "
            query += "UETR, "
            query += "Sender, "
            query += "Receiver, "
            query += "TransactionReference, "
            query += "OrderingAccount, "
            query += "OrderingName, "
            query += "OrderingStreet, "
            query += "OrderingCountryCityZip, "
            query += "BeneficiaryAccount, "
            query += "BeneficiaryName, "
            query += "BeneficiaryStreet, "
            query += "BeneficiaryCountryCityZip, "
            query += "SettlementDate, "
            query += "SettlementCurrency, "
            query += "SettlementAmount, "
            query += "InstructedCurrency, "
            query += "InstructedAmount, "
            query += "l1, "
            query += "l2, "
            query += "l3, "
            query += "l4, "
            query += "l5, "
            query += "l6, "
            query += "l7, "
            query += "Label) VALUES ("


            query += "$$" + str(data.iloc[i]["MessageId"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["Timestamp"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["UETR"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["Sender"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["Receiver"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["TransactionReference"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["OrderingAccount"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["OrderingName"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["OrderingStreet"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["OrderingCountryCityZip"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["BeneficiaryAccount"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["BeneficiaryName"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["BeneficiaryStreet"]) + "$$" + ", "
            query += "$$" + str(data.iloc[i]["BeneficiaryCountryCityZip"]) + "$$" + ", "

            sdate = str(data.iloc[i]["SettlementDate"])
            query += "$$" + "20" + sdate[0:2] + "-" + sdate[2:4] + "-" + sdate[4:6] + "$$" + ", "

            query += "$$" + str(data.iloc[i]["SettlementCurrency"]) + "$$" + ", "
            query += str(data.iloc[i]["SettlementAmount"]) + ", "

            query += "$$" + str(data.iloc[i]["InstructedCurrency"]) + "$$" + ", "
            query += str(data.iloc[i]["InstructedAmount"]) + ", "

            query += str(data_proc.iloc[i]["l1"]) + ", "
            query += str(data_proc.iloc[i]["l2"]) + ", "
            query += str(data_proc.iloc[i]["l3"]) + ", "
            query += str(data_proc.iloc[i]["l4"]) + ", "
            query += str(data_proc.iloc[i]["l5"]) + ", "
            query += str(data_proc.iloc[i]["l6"]) + ", "
            query += str(data_proc.iloc[i]["l7"]) + ", "

            query += str(int(data.iloc[i]["Label"]))

            query += ");"

            cur.execute(query)



    else:
        for i in range(len(data)):

            print("Loading",i+1,"from",len(data))
            

            query  = "INSERT INTO " + table_name + " ("

            midval = False
            for d in headers:
                if midval:
                    query += ", "
                query += d.lower()
                midval = True

            query += ")"
            query += " VALUES ("

            midval = False
            for d in data:
                if midval:
                    query += ", "

                dtype = pd.api.types.infer_dtype([data.iloc[i][d]])

                if dtype == "integer":
                    query += str(int(data.iloc[i][d]))
                elif dtype == "floating":
                    query += str(data.iloc[i][d])
                else:
                    query += "$$" + str(data.iloc[i][d]) + "$$"
                        
                midval = True

            for d in data_proc:
                if midval:
                    query += ", "

                dtype = pd.api.types.infer_dtype([data_proc.iloc[i][d]])

                if dtype == "integer":
                    query += str(int(data_proc.iloc[i][d]))
                elif dtype == "floating":
                    query += str(data_proc.iloc[i][d])
                else:
                    query += "$$" + str(data_proc.iloc[i][d]) + "$$"
                        
                midval = True


            query += ")"

            cur.execute(query)


    # Close connection
    cur.close()
    conn.close()







def main(argv):

    option = argv[1]

    records_to_upload = 10000

    if option == "bank":
        csv_path_bank = 'datasets/PETs/bank_swift/bank_dataset.csv'
        csv_path_bank_label = 'datasets/PETs/bank_swift/bank.csv'

        data_bank = pd.read_csv(csv_path_bank, nrows=records_to_upload)
        data_bank_label = pd.read_csv(csv_path_bank_label, nrows=records_to_upload)

        upload("bank", data_bank, data_bank_label)

    elif option == "swift_train":
        csv_path_swift_train = 'datasets/PETs/bank_swift/swift_transaction_train_dataset.csv'
        csv_path_swift_train_x = 'datasets/PETs/bank_swift/swift_train_x.csv'
        csv_path_swift_train_y = 'datasets/PETs/bank_swift/swift_train_y.csv'

        data_swift_train = pd.read_csv(csv_path_swift_train, nrows=records_to_upload)
        data_swiftp_train_x = pd.read_csv(csv_path_swift_train_x, nrows=records_to_upload)
        data_swiftp_train_y = pd.read_csv(csv_path_swift_train_y, nrows=records_to_upload)

        upload("swift_train", data_swift_train, data_swiftp_train_x, data_swiftp_train_y)

    elif option == "swift_test":

        csv_path_swift_test = 'datasets/PETs/bank_swift/swift_transaction_test_dataset.csv'
        csv_path_swift_test_x = 'datasets/PETs/bank_swift/swift_test_x.csv'
        csv_path_swift_test_y = 'datasets/PETs/bank_swift/swift_test_y.csv'

        data_swift_test  = pd.read_csv(csv_path_swift_test, nrows=records_to_upload)
        data_swiftp_test_x = pd.read_csv(csv_path_swift_test_x, nrows=records_to_upload)
        data_swiftp_test_y = pd.read_csv(csv_path_swift_test_y, nrows=records_to_upload)

        upload("swift_test", data_swift_test, data_swiftp_test_x, data_swiftp_test_y)




if __name__ == '__main__':
    main(sys.argv)
