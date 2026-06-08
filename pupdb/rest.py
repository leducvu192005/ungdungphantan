"""
    This module represent the RESTful HTTP interface to PupDB.
"""

import os
import json
import traceback
import threading
import urllib.request
import logging

from flask import Flask, request, Response, jsonify

from pupdb.core import PupDB


def replicate_to_slave(method, url_path, data=None):
    """ Helper function to replicate set/remove operation to slave node asynchronously. """
    slave_url = os.environ.get('PUPDB_SLAVE_URL')
    if not slave_url:
        return

    # Formulate URL
    base_url = slave_url.rstrip('/')
    url = base_url + url_path

    try:
        req = urllib.request.Request(url, method=method)
        if data is not None:
            req.add_header('Content-Type', 'application/json')
            req.data = json.dumps(data).encode('utf-8')

        with urllib.request.urlopen(req, timeout=5) as response:
            response.read()
    except Exception as e:
        logging.error('Error during replication to slave: %s', str(e))



# pylint: disable=too-many-ancestors
class CustomResponse(Response):
    """ Custom Response Class for the Flask Application. """

    # pylint: disable=arguments-differ
    @classmethod
    def force_type(cls, rv, environ=None):
        """ Overriden method to jsonify payload. """
        if isinstance(rv, dict):
            rv = jsonify(rv)
        return super(CustomResponse, cls).force_type(rv, environ)


def init_module():
    """ Initializes the Flask App. """

    dirpath = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(dirpath)
    view_dir = os.path.join(project_root, 'view')

    app = Flask(__name__, static_folder=view_dir, static_url_path='')
    app.response_class = CustomResponse
    database = PupDB(os.environ.get('PUPDB_FILE_PATH') or 'pupdb.json')
    return app, database


APP, DB = init_module()


def sync_from_slave_on_startup(db):
    """ Recover state from the configured slave node upon Master startup. """
    if os.environ.get('PUPDB_ROLE') == 'master':
        slave_url = os.environ.get('PUPDB_SLAVE_URL')
        if slave_url:
            logging.info("Attempting startup synchronization from Slave: %s", slave_url)
            try:
                url = slave_url.rstrip('/') + '/dumps'
                req = urllib.request.Request(url, method='GET')
                with urllib.request.urlopen(req, timeout=3) as response:
                    resp_data = response.read()
                    resp_json = json.loads(resp_data.decode('utf-8'))
                    if 'database' in resp_json:
                        slave_db = resp_json['database']
                        if slave_db:
                            logging.info("Synchronizing data from Slave. Overwriting Master DB with %s keys.", len(slave_db))
                            with db.process_lock:
                                db._flush_database_no_lock(slave_db)
                        else:
                            logging.info("Slave database is empty. No startup sync required.")
            except Exception as e:
                logging.error("Failed to sync from Slave on startup: %s", str(e))


sync_from_slave_on_startup(DB)


@APP.route('/', methods=['GET'])
def index():
    """ Serves the dashboard HTML file. """
    try:
        dirpath = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(dirpath)
        html_path = os.path.join(project_root, 'view', 'dashboard.html')
        with open(html_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content, 200, {'Content-Type': 'text/html; charset=utf-8'}
    except Exception as e:
        return 'Dashboard HTML not found or error loading: {}'.format(str(e)), 500


@APP.route('/get', methods=['GET'])
def db_get():
    """ Endpoint Function to interact with PupDB's get() method. """

    key = request.args.get('key')
    if not key:
        return {'error': 'Missing parameter \'key\''}, 400
    return {'key': key, 'value': DB.get(key)}, 200


@APP.route('/set', methods=['POST'])
def db_set():
    """ Endpoint Function to interact with PupDB's set() method. """

    try:
        key = request.json.get('key')
        value = request.json.get('value')
        if not key:
            return {'error': 'Missing parameter \'key\''}, 400
        if not value:
            return {'error': 'Missing parameter \'value\''}, 400

        result = DB.set(key, value)

        if result:
            if os.environ.get('PUPDB_ROLE') == 'master':
                threading.Thread(
                    target=replicate_to_slave,
                    args=('POST', '/set', {'key': key, 'value': value})
                ).start()
            return {
                'message': 'Key \'{}\' set to Value \'{}\''.format(key, value)
            }, 200
        return {
            'error':
            'There was a problem saving ({}, {}) to the DB.'.format(
                key, value
            )
        }, 400
    except Exception:
        return {'error': 'Unable to process this request.'}, 422


@APP.route('/remove/<key>', methods=['DELETE'])
def db_remove(key):
    """ Endpoint Function to interact with PupDB's remove() method. """

    try:
        if not key:
            return {'error': 'Missing parameter \'key\''}, 400

        try:
            result = DB.remove(key)
        except KeyError as key_err:
            return {'error': str(key_err)[1:-1]}, 404

        if result:
            if os.environ.get('PUPDB_ROLE') == 'master':
                threading.Thread(
                    target=replicate_to_slave,
                    args=('DELETE', '/remove/{}'.format(key))
                ).start()
            return {
                'message': 'Key \'{}\' removed from DB.'.format(key)
            }, 200

        return {
            'error':
            'There was a problem removing Key \'{}\' from the DB.'.format(key)
        }, 400
    except Exception:
        return {
            'error':
                'Unable to process this request. Details: %s' %
                traceback.format_exc(),
        }, 422


@APP.route('/keys', methods=['GET'])
def db_keys():
    """ Endpoint Function to interact with PupDB's keys() method. """

    return {'keys': list(DB.keys())}, 200


@APP.route('/values', methods=['GET'])
def db_values():
    """ Endpoint Function to interact with PupDB's values() method. """

    return {'values': list(DB.values())}, 200


@APP.route('/items', methods=['GET'])
def db_items():
    """ Endpoint Function to interact with PupDB's items() method. """

    return {'items': [list(item) for item in DB.items()]}, 200


@APP.route('/dumps', methods=['GET'])
def db_dumps():
    """ Endpoint Function to interact with PupDB's dumps() method. """

    return {'database': json.loads(DB.dumps())}, 200


@APP.route('/truncate-db', methods=['POST'])
def db_truncate():
    """ Endpoint Function to interact with PupDB's truncate_db() method. """

    result = DB.truncate_db()

    if result:
        return {
            'message': 'DB has been truncated successfully.'
        }, 200

    return {
        'error': 'There was a problem truncating the DB.'
    }, 400


@APP.after_request
def add_cors_headers(response):
    """ Allow cross-origin requests for dashboard integration. """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, DELETE, OPTIONS'
    return response
