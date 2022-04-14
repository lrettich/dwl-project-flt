import psycopg2
import sqlalchemy


class DBConnection:
    """
    Class to store the connection to the database

    ...

    Attributes
    ----------
    sqlalchemy_connection : sqlalchemy.engine.base.Engine
        Engine object with established sqlalchemy connection to the database
    psycopg2_connection : psycopg2.extensions.connection
        Connection object with established psycopg2 connection to the database
    user : str
        Username for the database connection
    password : str
        Password for the database connection
    endpoint : str
        Endpoint for the database connection
    db_name : str
        Databse name for the database connection

    Methods
    -------
    init_psycopg2_connection():
        Initializes a psycopg2 connection to the database using the attributes on the object itself.
    init_sqlalchemy_connection():
        Initializes a sqlalchemy connection to the database using the attributes on the object itself.
    """
    def __init__(self, user, password, endpoint, db_name):
        """
        Parameters
        ----------
        user : str
            Username for the database connection
        password : str
            Password for the database connection
        endpoint : str
            Endpoint for the database connection
        db_name : str
            Databse name for the database connection
        """
        self.user = user
        self.password = password
        self.endpoint = endpoint
        self.db_name = db_name
        
        # The connections using the sqlalchemy or the psycopg2 library are not established right away,
        # as they are not used both in every case.
        self.sqlalchemy_connection = None
        self.psycopg2_connection = None

    def init_psycopg2_connection(self):
        """Initializes a psycopg2 connection to the database using the attributes on the object itself."""
        conn_string = f'host={self.endpoint} dbname={self.db_name} user={self.user} password={self.password}'
        self.psycopg2_connection = psycopg2.connect(conn_string)
        self.psycopg2_connection.autocommit = True

    def init_sqlalchemy_connection(self):
        """Initializes a sqlalchemy connection to the database using the attributes on the object itself."""
        conn_string = f'postgresql://{self.user}:{self.password}@{self.endpoint}/{self.db_name}'
        self.sqlalchemy_connection = sqlalchemy.create_engine(conn_string)
