import MySQLdb
import random
import database_creds as dbc

#SQL

all_user_ids =  'SELECT DISTINCT(user_id) FROM raw_data'
test_raw_data = 'SELECT user_id, SUM(amount),  COUNT(DISTINCT event_id),  MAX(amount), MIN(amount) FROM raw_data WHERE user_id = %s'
test_agg_data = 'SELECT * FROM agg_data WHERE user_id = %s'


if __name__=="__main__":
    try:
        db = MySQLdb.connect(dbc.host, dbc.user, dbc.passwd, dbc.db_name)  
        cur = db.cursor()
    except:
        exit('Connection failed. Something went wrong')  

    cur.execute(all_user_ids)
    users=cur.fetchall()
    users = list(map(lambda x: x[0], users))
    random_users = random.sample(users, 10)

    for user in random_users:
        cur.execute( test_raw_data, (user,))
        from_raw_data = cur.fetchone()
        cur.execute(test_agg_data, (user,))
        from_agg_data = cur.fetchone()
        print(from_raw_data)
        print(from_agg_data)
        assert(from_raw_data==from_agg_data)
    print(random_users)
    

