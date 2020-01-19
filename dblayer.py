import logging
import sqlite3
from sqlite3 import Error

logger = logging.getLogger('grabber')
logger.setLevel(logging.INFO)
#ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#ch.setFormatter(formatter)
#logger.addHandler(ch)

def init_database():
    database = r"pythonsqlite.db"
    sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS reddit_posts (
                                            id          INTEGER PRIMARY KEY,
                                            postid text UNIQUE,
                                            date text,
                                            url text,
                                            subreddit text
                                        ); """

    logger.info("DB init started ...")
    conn = create_connection(database)
    if conn is not None:
            # create projects table
            create_table(conn, sql_create_projects_table)
    else:
            logger.error("cannot create the database connection.")
    logger.info("DB init completed")
    return conn



def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        logger.critical(e)

def add_post(conn, post):
    sql = ''' INSERT  INTO reddit_posts(postid,date,url,subreddit)
              VALUES(?,?,?,?) '''
    try:
      cur = conn.cursor()
      cur.execute(sql, post)
      conn.commit()
    except Error as e:
      logger.debug(post[0] + "already exists")
      #logging.error(e)
    return cur.lastrowid

def select_all_posts(conn):
  try:
    cur = conn.cursor()
    cur.execute("SELECT * FROM reddit_posts")
    rows = cur.fetchall()
  except Error as e:
    logging.error(e)
  for row in rows:
    print(row)

def get_pic_status(conn):
  try:
    cur = conn.cursor()
    cur.execute("SELECT count(1),subreddit FROM reddit_posts group by subreddit")
    rows = cur.fetchall()
  except Error as e:
    logging.error(e)
  for row in rows:
    print(row)

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        logger.error(e)