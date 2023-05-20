Usage
Create an instance of the Database class, providing the database credentials (username, password) and the database name.
Invoke the dbconnect method to create the database and establish a connection. It returns the connection object (engine).
(Optional) Use the check_database_connection method to verify if the database connection is established.
Invoke the load_data_to_db method to load datasets into the database, providing the dataset file path, tablename, and the connection object (engine).
Note: Ensure that you have the necessary dependencies (pandas, SQLAlchemy, and pymysql) installed before running the code.