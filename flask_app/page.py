import functools

from flask import (
    Blueprint, g, render_template
)

from flask_app.db import get_db
from flask_app.helpers import calculateSentimentData

bp = Blueprint('page', __name__, url_prefix='/')

@bp.route('')
def load():
    db = get_db()
    dbCursor = db.cursor()

    rows = dbCursor.execute('SELECT * FROM SEARCHES').fetchall()

    g.data = rows

    return render_template('page.html')

@bp.route('/all/')
def showAllStatistics():
    db = get_db()
    dbCursor = db.cursor()

    rows = dbCursor.execute('SELECT * FROM TWEETS').fetchall()

    sentimentData = calculateSentimentData(rows)

    geoData = sentimentData[0]
    yearData = sentimentData[1]

    return render_template('page2.html', searchName = "All Data", geoCA = geoData['geoCA'], geoUS=geoData['geoUS'], geoWORLD=geoData['geoWORLD'], yearData=yearData)

@bp.route('/data/<int:search_id>')
def showSearchStatistics(search_id):
    db = get_db()
    dbCursor = db.cursor()

    rows = dbCursor.execute('SELECT * FROM TWEETS WHERE queryId = ?', (search_id,)).fetchall()

    searchPrettyName = dbCursor.execute('SELECT prettyName FROM SEARCHES WHERE id = ?', (search_id,)).fetchone()['prettyName']

    sentimentData = calculateSentimentData(rows)

    geoData = sentimentData[0]
    yearData = sentimentData[1]

    return render_template('page2.html', searchName = searchPrettyName, geoCA = geoData['geoCA'], geoUS=geoData['geoUS'], geoWORLD=geoData['geoWORLD'], yearData=yearData)