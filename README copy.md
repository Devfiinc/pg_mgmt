# Database management via Python3

The scripts will either download online datasets or fetch them from the ones in "datasets" folder.
The datasets get processed and uploaded into the Postgres server with user "postgres".


### Datasets

 - Kaggle - Nerves

 - Kaggle - NKI

 - SKLearn - Iris

 - SmartNoise - PUMS

 - SmartNoise - Sample Random

 - TensorFlow - Mnist

 - TensorFlow Federated (TFF) - Emnist.

By running main.py all available datasets will be preprocessed and uploaded into Postgres.


### Postgres version

If you've more than one Postgres servers installed you'll need to set the port in database.ini. In our test environment we're running Postgres 12.9 and Postgres 14.4:

 - PG12 port = 5432

 - PG14 port = 5433

