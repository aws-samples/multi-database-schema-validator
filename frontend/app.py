import os
from flask import Flask, render_template, jsonify

from src.utility.utils import list_tasks as list_existing_tasks
from flask_cors import CORS, cross_origin
app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/')
def index():
    return render_template('home.html')


@app.route('/delete_tasks')
def delete_tasks():
    existing_tasks = list_tasks()
    return render_template('delete_tasks.html', tasks=existing_tasks)


@app.route('/list_tasks')
@cross_origin()
def list_tasks():
    existing_tasks = list_existing_tasks()
    return jsonify(existing_tasks)


@app.route('/generate_csv')
def generate_csv():
    pass


@app.route('/list_csvs')
def list_csvs():
    """
    List the CSVs that are currently present
    :return:
    """


@app.route('/set_config')
def set_config():
    """
    Set the various configs that are specified in the config files
    :return:
    """
