import sqlite3
import json

comment_dump = "/home/jonas/Development/reddit_comment_dump/RC_2008-07"
db_conn = sqlite3.connect("comment_data.sqlite")
index = 0

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

def row_with_field_name_exists(comment, field_name):
    c = db_conn.cursor()
    c.execute("SELECT COUNT(*) FROM comments WHERE parent_id = ?", comment[field_name])
    parent_in_db = c.fetchone()
    return (bool)parent_in_db

def insert_parent(comment):
    if row_with_field_name_exists(comment, "comment_id")
        #update
    else:
        #insert
        c = db_conn.cursor()
        c.execute("""
            INSERT INTO comments (parent_id, parent_body) VALUES (?,?)
        """, comment["parent_id"], comment["body"])

def insert_reply(comment):
    if row_with_field_name_exists(comment, "parent_id")
        #update
    else:
        #insert

def highest_score_reply(comment):
    c = db_conn.cursor()
    c.execute("SELECT reply_score FROM comments WHERE parent_id = ?", comment["parent_id"],)
    score_from_db = c.fetchone()
    if comment["score"] > score_from_db:
        return True
    else:
        return False

def valid_for_insert(comment):
    if (comment["score"] >= 2):
        if comment["parent_id"] == comment["subreddit_id"]:
            # Insert it as any other row
            #return True
            insert_parent(comment)
        elif highest_score_reply(comment):
            insert_reply(comment)
        else:
            return False


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

        if valid_for_insert(comment):
            insert_into_db(comment)

        if index >= 500:
            break

