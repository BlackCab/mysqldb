import MySQLdb
import random
import database_creds as dbc

#SQL

check_query =     'SELECT * FROM raw_data LIMIT 1;'
raw_data_insert = 'INSERT INTO raw_data (user_id, event_id, amount) VALUES (%s ,%s, %s);'

# Constants
rows_interval = (90000,100000)
user_id_interval = (1,100)
event_id_interval = (1,100)
amount_interval = (-100000, 100000)

def generate_random(i):
    while i>0:
        i-=1
        yield (random.randint(user_id_interval[0],user_id_interval[1]), random.randint(event_id_interval[0],event_id_interval[1]), random.randint(amount_interval[0],amount_interval[1]))


if __name__=='__main__':

    try:
        db = MySQLdb.connect(dbc.host, dbc.user, dbc.passwd, dbc.db_name)
        cur = db.cursor()
    except:
        exit('Connection failed. Something went wrong')  
   
    cur.execute(check_query)

    if not cur.fetchone():
        cur.executemany(raw_data_insert, generate_random(random.randint(rows_interval[0],rows_interval[1])))
        db.commit()
    else:
        exit('raw_data is full')
    
