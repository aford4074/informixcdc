# InformixCdc
Python module for Informix Change Data Capture

# build
python setup.py build_ext

# Prerequisites
* Informix CSDK installed on the client
* Client configured to connect to the instance you want to capture

# Informix setup
1. As informix run $INFORMIXDIR/etc/syscdcv1.sql to create the syscdcv1 database.
2. Create a database with logging enabled (optional)
3. Create a table (optional)

# testing the build without installing
Modify test() in informixcdc.py to change the config to meet your environment.

export PYTHONPATH=<source top level dir>/build/lib.<os specific dir>

cd <source top level dir>

python informixcdc.py

This *should* start InformixCdc on the table configured in test()

# Install

Untested

python setup.py install
