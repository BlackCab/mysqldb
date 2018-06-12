import MySQLdb
import database_creds as dbc


if __name__=='__main__':
    db = MySQLdb.connect(dbc.host, dbc.user, dbc.passwd, dbc.db_name)  
    cur = db.cursor()
    cur.execute('DELETE FROM raw_data;')  
    cur.execute('DELETE FROM agg_data;')
    cur.execute('DELETE FROM last_processed_id;')
    db.commit()
