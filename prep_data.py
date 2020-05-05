import sqlite3

limit = 3000

def write_to_file(filename):
    with open (filename) as f:
        for

db_conn = sqlite3.connect("reddit_commets.sqlite")
c = db_conn.cursor()

c.execute("SELECT parent_body, reply_body FROM comments WHERE id > {} AND parent_body != "" AND reply_body != "" ORDER BY id ASC LIMIT {}".format(current_index, limit))
rows = c.fetchall()

