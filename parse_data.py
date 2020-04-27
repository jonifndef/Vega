import sqlite3
import json
from enum import Enum

comment_dump = "/home/jonas/Development/reddit_comment_dump/RC_2015-12"
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

def insert_comment(comment):
    c = db_conn.cursor()
    if (get_comment_type(comment) == Comment_type.PARENT):
        c.execute("INSERT INTO comments (parent_id, comment_id, parent_body) VALUES (?,?,?)",comment["parent_id"],comment["comment_id"],comment["body"])
    else:
        if (is_highest_score_reply(comment)):
            c.execute("INSERT INTO comments (parent_id, comment_id, reply_score, reply_body) VALUES (?,?,?,?)",comment["parent_id"],comment["comment_id"],comment["score"],comment["body"])

def update_comment(comment):
    c = db_conn.cursor()
    if (get_comment_type(comment) == Comment_type.PARENT):
        c.execute("UPDATE comments SET parent_body = '?' WHERE parent_id = '?'",comment["body"],comment["comment_id"])
    else:
        c.execute("UPDATE comments SET reply_body = '?' WHERE parent_id = '?'",(comment["body"],comment["parent_id"],))

def comment_exists_in_db(comment):
    if (get_comment_type(comment) == Comment_type.PARENT):
        field_name = "comment_id"
    else:
        field_name = "parent_id"

    c = db_conn.cursor()
    c.execute("SELECT COUNT(*) FROM comments WHERE parent_id = ?",(comment[field_name],))
    num_in_db = c.fetchone()
    return bool(num_in_db)

def is_highest_score_reply(comment):
    c = db_conn.cursor()
    c.execute("SELECT reply_score FROM comments WHERE parent_id = ?", comment["parent_id"],)
    score_from_db = c.fetchone()
    if comment["score"] > score_from_db:
        return True
    else:
        return False

def verify_and_insert(comment):
    if (comment["score"] >= 2):
        # We potentially want to check for other things in the future
        if (comment_exists_in_db(comment)):
            update_comment(comment)
        else:
            insert_comment(commetn)

create_database()

with open (comment_dump) as f:
    for row in f:
        index += 1
        comment = json.loads(row)
        # Which fields are important?
        # - body, which the actual body of the comment
        # - score, we don't want comments that are too low rated
        # - parent_id
        # - id
        # - subreddit_id, if it's the same as parent_id, then it's a top level comment
        verify_and_insert(comment)

        if index >= 500:
            break

# INSERT INTO comments (parent_id, comment_id, reply) VALUES (?,?,?)
# INSERT INTO comments (parent_id, comment_id, parent) VALUES (?,?,?)

#if extits in db: update, else insert
# one function for each,
# format sql query with .format, depening on if it's parent or reply
