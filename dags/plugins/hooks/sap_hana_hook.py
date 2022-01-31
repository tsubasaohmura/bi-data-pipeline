from hdbcli import dbapi  # SAP HANA DB API

from airflow.hooks.base_hook import BaseHook


class SapHanaHook(BaseHook):
    """Very basic hook to SAP HANA Database.
    
    PROPERTIES:
        hana_conn_id (str): name of HANA connection in Connections

    FUNCTIONS:
        get_conn(): connect to HANA database through hana_conn_id in Connections
        query(sql): get connection and send query to HANA database
    """
    
    default_conn_name = 'hana_default'

    def __init__(self, hana_conn_id=default_conn_name):
        super().__init__(source=None)
        self.hana_conn_id = hana_conn_id


    def get_conn(self):
        """Connect to HANA database and return connection"""
        conn = self.get_connection(self.hana_conn_id)
        #connection = dbapi.connect(
        #    address=conn.host,
        #    port=conn.port,
        #    user=conn.login,
        #    password=conn.password
        #)
        
        ## This is workaround. PJI is migrating from Makuhari to Tokyo DC.
        ## We need to switch to QJH environment now.
        connection = dbapi.connect(
            address='10.212.51.15',
            port='30015',
            user='SI_HANA_COMBI_USER',
            password='Abcd2021#'
        )
        if not connection.isconnected():
            raise ConnectionError("Connection to HANA database unsuccessful. Please check your proxy and Airflow connection settings.")

        return connection


    def query(self, sql):
        """Send SQL query to HANA and return cursor"""
        conn = self.get_conn()
        cursor = conn.cursor()
        self.log.info("Sending query to HANA...")
        response = cursor.execute(sql)
        if response is False:
            raise Exception("HANA Cursor failed to execute SQL query.")
        return cursor

hana = SapHanaHook('hana_PJI')
