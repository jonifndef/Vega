import sqlite3
import json
from enum import Enum

comment_dump = "/home/jonas/Development/reddit_comment_dump/RC_2008-07"
db_conn = sqlite3.connect("comment_data.sqlite")
index = 0

class Comment_type(Enum):
    PARENT = 1
    REPLY = 2

def get_comment_type(comment):
    if comment["parent_id"] == comment["subreddit_id"]:
        return Comment_type.PARENT
    else:
        return Comment_type.REPLY

def create_database():
    c = db_conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS comments
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_id TEXT DEFAULT "",
            reply_id TEXT DEFAULT "",
            reply_score INTEGER DEFAULT -1,
            parent_body TEXT DEFAULT "",
            reply_body TEXT DEFAULT ""
        )
    """)
    c.close()

def format_body(body):
    body = body.replace("\n"," newlinechar ").replace("\r"," newlinechar ").replace("'",'"')
    return body

def insert_comment(comment):
    c = db_conn.cursor()
    if (get_comment_type(comment) == Comment_type.PARENT):
        c.execute("INSERT INTO comments (parent_id, comment_id, parent_body) VALUES (?,?,?)",comment["parent_id"],comment["id"],format_body(comment["body"]))
    else:
        if (is_highest_score_reply(comment)):
            c.execute("INSERT INTO comments (parent_id, reply_id, reply_score, reply_body) VALUES (?,?,?,?)",(comment["parent_id"],comment["id"],comment["score"],format_body(comment["body"]),))

def update_comment(comment):
    c = db_conn.cursor()
    if (get_comment_type(comment) == Comment_type.PARENT):
        c.execute("UPDATE comments SET parent_body = ? WHERE parent_id = ?",format_body(comment["body"]),comment["id"])
    else:
        c.execute("UPDATE comments SET reply_body = ?, reply_score = ?, reply_id = ? WHERE parent_id = ?",(format_body(comment["body"]),comment["score"],comment["id"],comment["parent_id"],))

def comment_exists_in_db(comment):
    if (get_comment_type(comment) == Comment_type.PARENT):
        field_name = "id"
    else:
        field_name = "parent_id"

    c = db_conn.cursor()
    c.execute("SELECT COUNT(*) FROM comments WHERE parent_id = ?",(comment[field_name],))
    num_in_db = c.fetchone()
    if num_in_db is None:
        return False
    return bool(num_in_db[0])

def is_highest_score_reply(comment):
    c = db_conn.cursor()
    c.execute("SELECT reply_score FROM comments WHERE parent_id = ?", (comment["parent_id"],))
    score_from_db = c.fetchone()
    if score_from_db is None:
        return True
    if comment["score"] > int(score_from_db):
        return True
    else:
        return False

def verify_and_insert(comment):
    if (comment["score"] >= 2):
        # We potentially want to check for other things in the future
        if (comment_exists_in_db(comment)):
            update_comment(comment)
        else:
            insert_comment(comment)

create_database()

with open (comment_dump) as f:
    for row in f:
        index += 1
        comment = json.loads(row)
        verify_and_insert(comment)

        if index >= 10000:
            break
    db_conn.commit()
