# This code connects to the sql database that extracted the
# data from MRP D365

import pandas as pd
import pyodbc

# Connect to the SQL database
conn = pyodbc.connect("<connection_string>")

# SQL query to fetch the data from the destination table
sql_query = "SELECT date, order_number, client_number, product_number, SKU, qty FROM destination_order_transactions"

# Read the data into a pandas DataFrame
df = pd.read_sql_query(sql_query, conn)

# Close the database connection
conn.close()
