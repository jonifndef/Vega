import sqlite3

current_index = 0
limit = 3000
filename_from = "test.from"
filename_to = "test.to"

db_conn = sqlite3.connect("comment_data.sqlite")
c = db_conn.cursor()

while True:
    c.execute("SELECT parent_body, reply_body FROM comments WHERE id > {} AND parent_body != '' AND reply_body != '' ORDER BY id ASC LIMIT {}".format(current_index, limit))
    rows = c.fetchall()

    if (len(rows) == 0):
        print("No more rows returned, extiting...")
        break
    if (current_index != 0):
        filename_from = "train.from"
        filename_to = "train.to"
    for row in rows:
        with open(filename_from, "a", encoding='utf8') as f:
            f.write(row[0] + '\n')
        with open(filename_to, "a", encoding='utf8') as f:
            f.write(row[1] + '\n')
    current_index += limit
    print("Updated current_index, value: {}".format(current_index))
