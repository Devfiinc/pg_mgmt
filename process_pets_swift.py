### Libraries for Data Handling

from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.preprocessing import StandardScaler, MinMaxScaler






pd.set_option("display.max_columns", None)

data_dir = "datasets/PETs/original/"
#data_swift_train = data_dir + "swift_transaction_train_dataset.csv"
#data_swift_test = data_dir + "swift_transaction_test_dataset.csv"
#data_bank = data_dir + "bank_dataset.csv"


records_to_process = 100000
train = pd.read_csv(data_dir + "swift_transaction_train_dataset.csv", index_col="MessageId", nrows=records_to_process)
train["Timestamp"] = train["Timestamp"].astype("datetime64[ns]")
test = pd.read_csv(data_dir + "swift_transaction_test_dataset.csv", index_col="MessageId", nrows=records_to_process)
test["Timestamp"] = test["Timestamp"].astype("datetime64[ns]")

train = pd.read_csv(data_dir + "swift_transaction_train_dataset.csv", index_col="MessageId")
train["Timestamp"] = train["Timestamp"].astype("datetime64[ns]")
test = pd.read_csv(data_dir + "swift_transaction_test_dataset.csv", index_col="MessageId")
test["Timestamp"] = test["Timestamp"].astype("datetime64[ns]")



# Add features for model training

# Hour
train["hour"] = train["Timestamp"].dt.hour
test["hour"] = test["Timestamp"].dt.hour

# Hour frequency for each sender
senders = train["Sender"].unique()
train["sender_hour"] = train["Sender"] + train["hour"].astype(str)
test["sender_hour"] = test["Sender"] + test["hour"].astype(str)
sender_hour_frequency = {}
for s in senders:
    sender_rows = train[train["Sender"] == s]
    for h in range(24):
        sender_hour_frequency[s + str(h)] = len(sender_rows[sender_rows["hour"] == h])

train["sender_hour_freq"] = train["sender_hour"].map(sender_hour_frequency)
test["sender_hour_freq"] = test["sender_hour"].map(sender_hour_frequency)


# Sender-Currency Frequency and Average Amount per Sender-Currency
train["sender_currency"] = train["Sender"] + train["InstructedCurrency"]
test["sender_currency"] = test["Sender"] + test["InstructedCurrency"]

sender_currency_freq = {}
sender_currency_avg = {}

for sc in set(
    list(train["sender_currency"].unique()) + list(test["sender_currency"].unique())
):
    sender_currency_freq[sc] = len(train[train["sender_currency"] == sc])
    sender_currency_avg[sc] = train[train["sender_currency"] == sc][
        "InstructedAmount"
    ].mean()

train["sender_currency_freq"] = train["sender_currency"].map(sender_currency_freq)
test["sender_currency_freq"] = test["sender_currency"].map(sender_currency_freq)

train["sender_currency_amount_average"] = train["sender_currency"].map(
    sender_currency_avg
)
test["sender_currency_amount_average"] = test["sender_currency"].map(sender_currency_avg)



# Sender-Receiver Frequency
train["sender_receiver"] = train["Sender"] + train["Receiver"]
test["sender_receiver"] = test["Sender"] + test["Receiver"]

sender_receiver_freq = {}

for sr in set(
    list(train["sender_receiver"].unique()) + list(test["sender_receiver"].unique())
):
    sender_receiver_freq[sr] = len(train[train["sender_receiver"] == sr])

train["sender_receiver_freq"] = train["sender_receiver"].map(sender_receiver_freq)
test["sender_receiver_freq"] = test["sender_receiver"].map(sender_receiver_freq)



columns_to_drop = [
    "UETR",
    "Sender",
    "Receiver",
    "TransactionReference",
    "OrderingAccount",
    "OrderingName",
    "OrderingStreet",
    "OrderingCountryCityZip",
    "BeneficiaryAccount",
    "BeneficiaryName",
    "BeneficiaryStreet",
    "BeneficiaryCountryCityZip",
    "SettlementDate",
    "SettlementCurrency",
    "InstructedCurrency",
    "Timestamp",
    "sender_hour",
    "sender_currency",
    "sender_receiver",
]

train = train.drop(columns_to_drop, axis=1)
test = test.drop(columns_to_drop, axis=1)

train[train["Label"] == 1]
test[test["Label"] == 1]


Y_train = train["Label"].values
X_train = train.drop(["Label"], axis=1).values
Y_test = test["Label"].values
X_test = test.drop(["Label"], axis=1).values


scaler = StandardScaler()
scaler.fit(X_train)

X_train = scaler.transform(X_train)
X_test = scaler.transform(X_test)


np.savetxt("datasets/PETs/processed/swift_train_x.csv", X_train, delimiter=",", fmt='%.10f')
np.savetxt("datasets/PETs/processed/swift_train_y.csv", Y_train, delimiter=",", fmt='%.1f')

np.savetxt("datasets/PETs/processed/swift_test_x.csv", X_test, delimiter=",", fmt='%.10f')
np.savetxt("datasets/PETs/processed/swift_test_y.csv", Y_test, delimiter=",", fmt='%.1f')