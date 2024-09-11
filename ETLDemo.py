import configparser
import sys
import requests
import json
import datetime
import decimal
import petl
import pymysql
############# read from config file##################
config = configparser.ConfigParser()
try:
    config.read('C:\\Users\\Gymnasiet\\Desktop\\ETL\\ETLDemo-master\\ETLDemo-master\\ETLDemo.ini')
except Exception as e:
    print('Could not read configuration file: ' + str(e))
    sys.exit()


# extract information from config file
startdate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
server =config['CONFIG']['server']
user = config['CONFIG']['user'] 
password = config['CONFIG']['password']
database = config['CONFIG']['database']

#############  request data from url ##################
try:
    BOCResponse = requests.get(url + startdate)
    BOCResponse.raise_for_status()  # Check if the request was successful
except Exception as e:
    print('Could not make request: ' + str(e))
    sys.exit()

BOCDates = []
BOCRates = []

if BOCResponse.status_code == 200:
    BOCRaw = json.loads(BOCResponse.text)

    # Extract observation data into column arrays
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))

    # Create petl table from column arrays and rename the columns
    exchangeRates = petl.fromcolumns([BOCDates, BOCRates], header=['date', 'rate'])

    # Load expense document
    try:
        expenses = petl.io.xlsx.fromxlsx('C:\\Users\\Gymnasiet\\Desktop\\ETL\\ETLDemo-master\\ETLDemo-master\\Expenses.xlsx', sheet='Github')
    except Exception as e:
        print('Could not open expenses.xlsx: ' + str(e))
        sys.exit()


    # Join tables
    expenses = petl.outerjoin(exchangeRates, expenses, key='date')
    print(expenses)
    # Fill down missing values
    expenses = petl.filldown(expenses, 'rate')

    # Remove dates with no expenses
    expenses = petl.select(expenses, lambda rec: rec.USD is not None)

    # Add CAD column
    expenses = petl.addfield(expenses, 'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)

    # Print the data to verify
    print(expenses)


    # Initialize database connection
    try:
        dbConnection = pymysql.connect(
            host=server,
            database=database,
            user=user,
            password=password
        )
        print('Connected to database.')
    except pymysql.MySQLError as e:
        print('Could not connect to database: ' + str(e))
        sys.exit()

    # Define function to insert data into the database
    def insert_data(expenses, db_connection):
        try:
            with db_connection.cursor() as cursor:
                # Create an insert query
                query = """
                    INSERT INTO Expenses (date, rate, USD, CAD)
                    VALUES (%s, %s, %s, %s)
                """
                # Prepare data
                data = petl.records(expenses)
                # Execute query for each row
                for row in data:
                    cursor.execute(query, (row['date'], row['rate'], row['USD'], row['CAD']))
                # Commit the transaction
                db_connection.commit()
                print('Data inserted successfully.')
        except pymysql.MySQLError as e:
            print('Could not insert data into database: ' + str(e))

    # Populate Expenses database table
    try:
        insert_data(expenses, dbConnection)
    except Exception as e:
        print('Could not write to database: ' + str(e))

    # Close the connection
    dbConnection.close()