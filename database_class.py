from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd

class Database(object):
    def __init__(self, username, password, db_name):
        self.username = username
        self.password = password
        self.db_name = db_name

    def dbconnect(self):
        """     
        Function to create database and connection.
    
        Parameters:
        dataset_path_name (str): Path to the dataset / file
        tablename (str): Tablename for the data within the database
        engine (str): Connection object to access database
    
        Returns:
        None
        """
        # URL for DB, username, password and DB name
        # User has to send db username, password and db name on function call
        self.url = "mysql+pymysql://" + self.username + ":" + self.password + "@localhost/" + self.db_name

        # Create an engine object
        self.engine = create_engine(self.url, connect_args= dict(host='localhost', port=3306), echo=True)
        
        # Create db if it does not exist
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
            print("Databse " + self.db_name + " was successfully created.")
        else:
            # Connect the database if exists.
            self.engine.connect()
            print("Database already exists, connection was established.")    
        
        return self.engine.connect()
          
    def loadDatatoDB(dataset_path_name, tablename, engine):
        """
        Function loads datasets into database. The user can chose table names separately.
    
        Parameters:
        dataset_path_name (str): Path to the dataset / file
        tablename (str): Tablename for the data within the database
        engine (str): Connection object to access database
    
        Returns:
        None
        """  
        try:
           # Open dataset and safe as df
           with open(dataset_path_name, 'r') as file:
               df = pd.read_csv(file)
               # Load df into db
               df.to_sql(
                   tablename,
                   engine,
                   if_exists='replace',
                   index=True,
                   chunksize=500
               )
        except FileNotFoundError as e:
           # print error message
           print(e)
           print("Please check file path and file name")
        else:
           print("File read and loaded")