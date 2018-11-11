# Trans-Buddy
Translations inserter/updated from CSV to DB
Requires mysql.connector installation:
pip install http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-2.0.4.zip


# Insert to local env (without update):
./trans_buddy.py --path="./csv_files/ccc.csv"
# Update example:
./trans_buddy.py --path="./csv_files/ccc.csv" -u -t 10.200.5.218 
