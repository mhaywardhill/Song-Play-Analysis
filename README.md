# Project Overview

In this project, we create a database schema in Postgres and build an ETL pipeline using Python.  

A startup collects songs and user activity on their music streaming app. The analytics team wants to know what songs users are listening to. The data resides in a directory of JSON logs on user activity, as well as a directory with JSON metadata on the songs in their app.

The first dataset (in folder data) is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/).  

## Database Schema

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;![ER](/ER_diagram.PNG)



## Step 1: Setup the Environment

1. Set environments variables:

  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;PGHOSTADDR with the IP address of the Postgres host, and PGPASSWORD with the password

2. Clone the repo:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`https://github.com/mhaywardhill/Song-Play-Analysis.git`

3. Setup the Python virtual environment:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`conda create -n sparkify python=3.6`  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`conda activate sparkify`  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`pip install -r requirements.txt`  

## Step 2: Run the ETL

1. Create database sparkifdb and database schema:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`python ./create_tables.py`  

2. Run the ETL:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`python ./etl.py`

# Project Files  
<b>sql_queries.py</b>: Contains SQL queries for dropping and creating the fact and dimension tables. Also, it contains the SQL queries to load the dimensions.  

<b>create_tables.py</b>: Contains code for creating the sparkifydb database, and the database schema (the tables).  

<b>etl.py</b>: Runs the Python ETL.  

<b>Manual_ETL.ipynb</b>: Jupyter notebook to manually run the ETL step-by-step.

