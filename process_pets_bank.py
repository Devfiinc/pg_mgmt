### Libraries for Data Handling

from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.preprocessing import StandardScaler, MinMaxScaler

records_to_process = None


pd.set_option("display.max_columns", None)

data_dir = "datasets/PETs/original/"
#data_swift_train = data_dir + "swift_transaction_train_dataset.csv"
#data_swift_test = data_dir + "swift_transaction_test_dataset.csv"
#data_bank = data_dir + "bank_dataset.csv"



bank = pd.read_csv(data_dir + "/bank_dataset.csv", nrows=records_to_process)

bank["Flag"] = np.where(bank["Flags"] > 0, 1, 0)

columns_to_drop = [
    "Bank",
    "Account",
    "Name",
    "Street",
    "CountryCityZip",
    "Flags"
]


bank = bank.drop(columns_to_drop, axis=1)

bank_np = bank["Flag"].values



np.savetxt("datasets/PETs/processed/bank.csv", bank_np, delimiter=",", fmt='%.10f')