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
    #c.execute("""
    #    CREATE TABLE IF NOT EXISTS comments
    #    (
    #        id INTEGER PRIMARY KEY AUTOINCREMENT,
    #        parent_id TEXT DEFAULT "",
    #        reply_score INTEGER DEFAULT -1,
    #        parent_body TEXT DEFAULT "",
    #        reply_body TEXT DEFAULT ""
    #    )
    #""")
    c.close()

def format_body(body):
    body = body.replace("\n"," newlinechar ").replace("\r"," newlinechar ").replace('"',"'")
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

### New functions ###

def reply_exists(comment):
    c = db_conn.cursor()
    c.execute("SELECT CASE WHEN parent_id = ? THEN 1 ELSE 0 END FROM comments", (comment["id"],))
    reply_exists = c.fetchone()
    if (reply_exists is None):
        return False
    elif (bool(reply_exists[0])):
        return True
    else:
        return False

def insert_on_reply_row(comment):
    c = db_conn.cursor()
    c.execute("UPDATE comments SET parent = ? WHERE parent_id = ?", (format_body(comment["body"]),comment["id"],))

def insert_as_new_parent(comment):
    c = db_conn.cursor()
    c.execute("INSERT INTO comments (parent_id, parent_body) VALUES (?,?)", (comment["id"],format_body(comment["body"]),))

def comment_with_same_parent_exits(comment):
    c = db_conn.cursor()
    c.execute("SELECT CASE WHEN parent_id = ? THEN 1 ELSE 0 END FROM comments", (comment["parent_id"],))
    comment_exists = c.fetchone()
    if (comment_exists is None):
        return False
    elif (bool(comment_exists[0])):
        return True
    else:
        return False

def has_higher_score_than_existing(comment):
    c = db_conn.cursor()
    c.execute("SELECT reply_score FROM comments WHERE parent_id = ?", (comment["parent_id"],))
    score_from_db = c.fetchone()
    if score_from_db is None:
        return True
    if comment["score"] > int(score_from_db):
        return True
    else:
        return False

def update_reply(comment):
    c = db_conn.cursor()
    c.execute("UPDATE comments SET reply_body = ?, reply_score = ? WHERE parent_id = ?",(format_body(comment["body"]),comment["score"],comment["parent_id"],))

def insert_as_new_reply(comment):
    c = db_conn.cursor()
    c.execute("INSERT INTO comments (parent_id, reply_score, reply_body) VALUES (?,?,?)", (comment["parent_id"],comment["score"],format_body(comment["body"]),))

### new again ###

def comment_is_corrent_len(comment):
    body_string = format_body(comment["body"])
    if (len(body_string.split(" ")) > 50 or (len(body_string) < 1) or (len(body_string) > 1000)):
        return False
    else:
        return True

def get_parent_body(comment):
    c = db_conn.cursor()
    # is this really correct? Selecting "reply"?
    c.execute("SELECT reply FROM comments WHERE reply_id = ?", comment["parent_id"],)
    parent_body = c.fetchone()
    if parent_body != None:
        return parent_body[0]
    else:
        return False

def find_existing_score(comment):
    c = db_conn.cursor()
    c.execute("SELECT reply_score FROM comments WHERE parent_id = ?", comment["parent_id"],)
    score = c.fetchone()
    if score != None:
        return score[0]
    else:
        return False

def verify_and_insert(comment):
    paired_rows = 0
    parent_body = get_parent_body(comment)
    if (comment["score"] >= 2 and comment["body"] != "[deleted]" and comment_is_corrent_len(comment)):
        # We potentially want to check for other things in the future
        #if (comment_exists_in_db(comment)):
        #    print("updating existing row")
        #    update_comment(comment)
        #else:
        #    insert_comment(comment)

        #if (reply_exists(comment)):
        #    insert_on_reply_row(comment)
        #else:
        #    insert_as_new_parent(comment)
        #if (comment_with_same_parent_exits(comment)):
        #    if (has_higher_score_than_existing(comment)):
        #        update_reply(comment)
        #else:
        #    insert_as_new_reply(comment)

        existing_comment_score = find_existing_score(comment)
        if existing_comment_score:
            if comment["score"] > existing_comment_score:
                sql_insert_replace_comment()
        else:
            if parent_data:
                sql_insert_has_parent()
                paired_rows += 1
            else
                sql_insert_no_parent()

create_database()
with open (comment_dump) as f:
    for row in f:
        index += 1
        comment = json.loads(row)
        verify_and_insert(comment)
        #if index >= 1000000000:
        #    break
    db_conn.commit()

#   get comment
#   if another comment is a reply to this one, this comment_id will be an existing parent_id in the db
#       if so, insert this as parent
#       if not, it can still be a parent, only we havn't got any replies to it yet
#       maybe insert as parent, with it's comment_id as the parent_id?
#
#   if another comment in database has the same parent_id as this current comment,
#       does the current one have a higher score than one already in the db?
#           if so, replace the existing one with the current one
#       otherwise, do nothing
#   otherwise, insert this new one as a reply with it's parent_id as the parent_id of the row
#
#   Two scenarios: insert it as a reply, because it has a parent_id of some sort.
#       Then, everytime we insert a new
#
#
#
#
#
#
#
#
#
#
#
#
#
#
