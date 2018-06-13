import MySQLdb
import database_creds as dbc

#SQL
last_processed_id_zero =   'INSERT INTO last_processed_id (id) VALUES (0)'
last_processed_id_update = 'UPDATE last_processed_id SET id = %s;'
all_user_ids =             'SELECT DISTINCT(user_id) FROM raw_data;'
set_rank =                 'SET @rank=0;'
find_row_number =          "SELECT rank from (select @rank:=@rank+1 'rank', id from raw_data) AS T WHERE id = %s;"
group_data =               'SELECT SUM(amount),  COUNT(DISTINCT event_id),  MAX(amount), MIN(amount), user_id FROM raw_data GROUP BY user_id LIMIT %s , %s;'
agg_data_update =          'UPDATE agg_data SET balance=balance+%s, event_number = event_number+%s, best_event = %s, worst_event=%s WHERE user_id = %s;'
find_id_by_row =           'SELECT id FROM raw_data LIMIT %s, 1;'
last_processed_id_select = 'SELECT id FROM last_processed_id LIMIT 1'
agg_data_insert_ids =      'INSERT INTO agg_data (user_id) VALUES (%s);'
check_agg_data =           'SELECT * FROM agg_data LIMIT 1'
count_all_rows =           'SELECT COUNT(*) FROM raw_data;'

ROW_NUM = 10000


def check_agg_empty():
    user_ids = []
    cur.execute(all_user_ids)
    for uid in cur:
        user_ids.append(uid)
    cur.executemany(agg_data_insert_ids, user_ids)
    db.commit()


def get_rows(number_of_rows, last_id):
    cur.execute(set_rank)
    cur.execute(find_row_number, last_id)
    last_row_number = cur.fetchone()
    print(last_id, last_row_number)
    cur.execute(group_data, (last_row_number[0]-1, last_row_number[0]+number_of_rows))
    print(cur.fetchall())
    cur_n=db.cursor()
    cur_n.executemany(agg_data_update, cur)
    db.commit()
    return last_row_number
    

if __name__=='__main__':

    try:
        db = MySQLdb.connect(dbc.host, dbc.user, dbc.passwd, dbc.db_name)
        cur = db.cursor()
    except:
        exit('Connection failed. Something went wrong') 

    cur.execute(check_agg_data)
    if not cur.fetchone():
        check_agg_empty()    

    cur.execute(last_processed_id_select)
    last_id = cur.fetchone()
    if not last_id:
        cur.execute(last_processed_id_zero)
        cur.execute(find_id_by_row, (0,))
        last_id = cur.fetchone()
        cur.execute(last_processed_id_update, last_id)
        db.commit()

    while last_id:
        last_row_number = get_rows(ROW_NUM, last_id)
        cur.execute(find_id_by_row, (last_row_number[0]+ROW_NUM-1,))
        last_id = cur.fetchone()
        print(last_id)
        if last_id:
            cur.execute(last_processed_id_update, last_id)
        else:
            cur.execute(count_all_rows)
            rows_num = cur.fetchone()
            cur.execute(find_id_by_row, (rows_num[0]-1,))
            last_id = cur.fetchone()
            cur.execute(last_processed_id_update, last_id)
            last_id = None
        db.commit() 
 
    print("FINISH")
  #  db.commit()
    
    
    
    
   
    

