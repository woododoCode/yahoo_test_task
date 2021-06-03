import sqlite3

import flask
from flask import jsonify

from get_market_data import get_market_jsonified_data, load_market_data_to_db

app = flask.Flask(__name__)
app.config['JSON_SORT_KEYS'] = False


@app.route('/', methods=['GET'])
def home():
    return


@app.route('/api/v1/market/<string:market_name>', methods=['GET'])
def get_market_data(market_name):
    response = get_market_jsonified_data(market_name)
    return jsonify(response)


@app.route('/api/v1/load-data', methods=['GET'])
def load_data():
    try:
        load_market_data_to_db()
    except sqlite3.OperationalError:
        return jsonify('DATA IS LOADED ALREADY')
    return jsonify('OK')


if __name__ == '__main__':
    app.run()
