import sqlite3

from flask import current_app, g


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect('tweetDbTest.sqlite3')
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

def init_app(app):
    get_db()
    app.teardown_appcontext(close_db)