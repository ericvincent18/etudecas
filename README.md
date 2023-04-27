# Do the following steps to run this example :

1. Activate virtualenv - python version >= 3.8.13
run `python3 -m venv virtualenv in the root directory`
run `source virtualenv/bin/activate`
2. Install requirements
run `pip install -r requirements.txt`

3. If running this code on a mac os environement => Ventura 13.1, you will need to follow this guide to fix the bug:
https://github.com/elastic/elasticsearch/issues/91159

4. You will then need to configure your elasticsearch yml and keystore (sometimes creating the files if applicable)

5. Replace configs.py with your correct credentials
6. Run from the elasticsearch installatin directory : run `brew services start elasticsearch-full`
   Check if this service has successfully started with run `brew services list` on mac os
7. Once the elasticsearch cluster is started, you will need to log in with the username and password you have configured

8. PySpark will only connect to elasticsearch if elasticsearch has started successfully beforehand. Replace the     configurations in main.py for the spark session.

9. Run on debug from main.py to see the creation of dummy data, data transformation, and data loading and qeurying to/from elasticssearch

10. the `get_data_from_sql.py` is simply to show the code needed to query to sql database that has the data queried from
   MRP D365. PySpark would connect normally to query the data.
