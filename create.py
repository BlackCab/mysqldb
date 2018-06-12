import MySQLdb
import database_creds

#SQL

create_db =                       'CREATE DATABASE IF NOT EXISTS {};'
create_table_raw_data =           'CREATE TABLE raw_data (id int AUTO_INCREMENT , user_id int, event_id int,amount int, PRIMARY KEY (id));'
create_table_agg_data =           'CREATE TABLE agg_data (user_id int, balance int DEFAULT 0, event_number int DEFAULT 0, best_event int, worst_event int, PRIMARY KEY (user_id));'
create_table_last_processed_id =  'CREATE TABLE last_processed_id (id int);'




tables = {'raw_data': create_table_raw_data, 'agg_data': create_table_agg_data, 'last_processed_id': create_table_last_processed_id}

def create_table(table):
    try:
        cur.execute(tables[table])
    except:
        print("Table {} alredy exists".format(table))    



if __name__ == '__main__':
    try:
        db = MySQLdb.connect(database_creds.host, database_creds.user, database_creds.passwd)
        cur = db.cursor()
    except:
        exit('Connection failed. Something went wrong')

    cur.execute(create_db.format(database_creds.db_name))
    cur.execute('USE {};'.format(database_creds.db_name))  
    
    for tab in tables.keys():
        create_table(tab)  
	

