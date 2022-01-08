import cassandra_functions
# import json
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from logger import App_Logger
lg = App_Logger()
logfile = open('./logs/cassandra.log', 'a')

config = {'zipp':"cassandra_functions\secure-connect-shohure-kotha.zip", 
        'client_id': "KxTmgOUcxAqndeZTknsicwSs" , 
        'client_secret':"NlmkH8Xo+BUf2Cl2geO4xQYPM7iFpZ4C96aoy4ZgkJWjCdSiBuJhsmUad6_H8J79UC..QrShmN7CY,njnxAqCuBRWBNi88ZZmzT36Z7cg2.J0lx2iF4yT9,0gUZWLfA9",
        'keyspace':'shohurekotha'	
         }



class cass_operation:
    def __init__(self, tab):
        self.zip = config['zipp']
        self.client_id = config['client_id']
        self.client_secret = config['client_secret']
        self.keyspace = config['keyspace']
        self.tab = tab

    def create_tab_connect(self,  column):     #table creation and db conenction in cassandra

        cloud_config = {
            'secure_connect_bundle': str(self.zip)
        }
        auth_provider = PlainTextAuthProvider(str(self.client_id),
                                              str(self.client_secret))
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect(str(self.keyspace))
        query = f" CREATE TABLE {self.tab}({column});"

        row = session.execute(query).one()
        print(row)
        print('database connected and table created')
        lg.log(logfile, f'cassandra db:'+
                f'\n database connected and {self.tab} has been created')

    def cass_insert_data(self, column,val):  #inserting data to cass db
        cloud_config = {
            'secure_connect_bundle': str(self.zip)
        }
        auth_provider = PlainTextAuthProvider(str(self.client_id),
                                              str(self.client_secret))
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect(str(self.keyspace))
        query = f" insert into {self.tab} ({column}) values({val});"

        row = session.execute(query).one()
        print(row)
        print(f'{val} are inserted into {self.keyspace}.{self.tab}')
        lg.log(logfile, f'cassandra db:'+
                f'\n {val} are inserted into {self.keyspace}.{self.tab}')






    def read_data(self):
        cloud_config = {
            'secure_connect_bundle': str(self.zip)
        }
        auth_provider = PlainTextAuthProvider(str(self.client_id),
                                              str(self.client_secret))
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect(str(self.keyspace))
        query = f"select * from {self.keyspace}.{self.tab};"
        exe = session.execute(query)
        col = exe.column_names

        data = pd.DataFrame()

    # def cass_update_tab(self, set_condition, where_condition):       # cass tab update
    #     cloud_config = {
    #         'secure_connect_bundle': str(self.zip)
    #     }
    #     auth_provider = PlainTextAuthProvider(str(self.client_id),
    #                                           str(self.client_secret))
    #     cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    #     session = cluster.connect(str(self.keyspace))
    #     query = f"UPDATE {self.keyspace}.{self.tab} SET {set_condition} WHERE {where_condition};"

    #     row = session.execute(query).one()
    #     result = f'{self.keyspace}.{self.tab} is updated successfuly'
    #     print(result)
    #     lg.log(logfile, result)

    # def cass_bulk_up(self, path, columns ):

    #     cloud_config = {
    #         'secure_connect_bundle': str(self.zip)
    #     }
    #     auth_provider = PlainTextAuthProvider(str(self.client_id),
    #                                           str(self.client_secret))
    #     cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)

    #     if type(columns) == dict:
    #         col_n_col_type = []
    #         for key, value in columns.items():
    #             col_n_col_type.append(key + ' ' + value)

    #         session = cluster.connect(str(self.keyspace))
    #         qry = f"CREATE TABLE IF NOT EXISTS {self.keyspace}.{self.tab} ({', '.join([i for i in col_n_col_type])});"
    #         print(qry)
    #         session.execute(qry)
    #         result = f'{self.keyspace}.{self.tab} has been created'
    #         lg.log(logfile, result)
    #         print(result)

    #     # inserting data from csv

    #     col = []
    #     for key, values in columns.items():
    #         col.append(key)

    #     with open(str(path), 'r') as data:
    #         file = csv.reader(data, delimiter=',')
    #         next(file)
    #         for i in file:
    #             query = f" insert into {self.keyspace}.{self.tab} ({', '.join([i for i in col])}) values({', '.join([value for value in i])});"
    #             session.execute(query)
    #             print(f'data{[value for value in i]} is inserted')
    #         lg.log(logfile, f'data has been inserted into {self.keyspace}.{self.tab}')


    # def delete_cass(self, condition):
    #     cloud_config = {
    #         'secure_connect_bundle': str(self.zip)
    #     }
    #     auth_provider = PlainTextAuthProvider(str(self.client_id),
    #                                           str(self.client_secret))
    #     cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    #     session = cluster.connect(str(self.keyspace))
    #     query = f"DELETE FROM {self.keyspace}.{self.tab} {condition};"

    #     row = session.execute(query).one()
    #     result = f'row {condition} has been deleted from {self.keyspace}.{self.tab}'
    #     print(result)
    #     lg.log(logfile, result)


    # def download_data(self, path):
    #     cloud_config = {
    #         'secure_connect_bundle': str(self.zip)
    #     }
    #     auth_provider = PlainTextAuthProvider(str(self.client_id),
    #                                           str(self.client_secret))
    #     cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    #     session = cluster.connect(str(self.keyspace))
    #     query = f"select * from {self.keyspace}.{self.tab};"
    #     exe = session.execute(query)
    #     col = exe.column_names
    #     c1 = []
    #     for i in exe:
    #         c1.append(i)

    #     c = []
    #     c.append(col)
    #     for i in c1:
    #         c2 = []
    #         for j in range(0, len(c1) + 1):
    #             c2.append(i[j])

    #         c.append(c2)
    #     with open(path, 'w', newline='\n') as file:
    #         f = csv.writer(file)
    #         f.writerows(c)
    #         result = 'file has been downloaded'
    #         lg.log(logfile, result)
    #         print(result)
    #         return jsonify(result)

        # result = f'row {condition} has been deleted from {self.keyspace}.{self.tab}'
        # print(result)
        # lg.log(logfile, result)

def cassandra_operations(table, column_names, value):
    """ function for uploading data to cassandra

    """
    cass = cass_operation(table)
    cass.create_tab_connect(column_names)
    cass.cass_insert_data(column_names, value)