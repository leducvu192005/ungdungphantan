"""
    This module represents the distributed sharding router proxy for PupDB.
"""

import os
import json
import urllib.request
import urllib.parse
import urllib.error
import logging

# pyrefly: ignore [missing-import]
from flask import Flask, request, Response, jsonify, g

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(process)d | %(levelname)s | %(message)s'
)


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
    return app


APP = init_module()


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


def get_shards():
    """ Returns the list of configured shard URLs. """
    shards_env = os.environ.get('PUPDB_SHARDS')
    if shards_env:
        return [s.strip() for s in shards_env.split(',') if s.strip()]
    # Default fallback for testing or standalone setup
    return ['http://127.0.0.1:4001', 'http://127.0.0.1:4002']


def get_shard_slaves_map():
    """ Returns a mapping of Master Shard URL to Slave Shard URL. """
    shards = get_shards()
    slaves_env = os.environ.get('PUPDB_SLAVES')
    if slaves_env:
        slaves = [s.strip() for s in slaves_env.split(',') if s.strip()]
    else:
        # Default fallback corresponding to default master shards
        slaves = ['http://127.0.0.1:4011', 'http://127.0.0.1:4012']

    mapping = {}
    for i, master_url in enumerate(shards):
        if i < len(slaves):
            mapping[master_url] = slaves[i]
    return mapping


def get_shard_url(key, shards):
    """ Hashes the key using sum of its characters' ASCII values to select a shard. """
    if not key or not shards:
        return None
    total_ascii = sum(ord(c) for c in str(key))
    index = total_ascii % len(shards)
    return shards[index]


def _do_forward(url, method, headers=None, data=None):
    """ Performs the actual HTTP request forwarding. """
    req = urllib.request.Request(url, method=method)
    if headers:
        for k, v in headers.items():
            if k.lower() in ('host', 'content-length'):
                continue
            req.add_header(k, v)

    if data is not None:
        if not req.get_header('Content-Type'):
            req.add_header('Content-Type', 'application/json')
        req.data = data

    with urllib.request.urlopen(req, timeout=10) as response:
        status_code = response.status
        resp_data = response.read()
        try:
            resp_json = json.loads(resp_data.decode('utf-8'))
            return resp_json, status_code
        except Exception:
            return resp_data.decode('utf-8'), status_code


def forward_request(url, method, headers=None, data=None):
    """ Forwards the HTTP request to the selected shard, falling back to its Slave if Master is down. """
    try:
        return _do_forward(url, method, headers, data)
    except urllib.error.HTTPError as e:
        try:
            err_data = e.read()
            err_json = json.loads(err_data.decode('utf-8'))
            return err_json, e.code
        except Exception:
            return {'error': e.reason}, e.code
    except Exception as e:
        # Connection failed, try failover to slave
        slaves_map = get_shard_slaves_map()
        for master_url, slave_url in slaves_map.items():
            if url.startswith(master_url):
                fallback_url = url.replace(master_url, slave_url, 1)
                logging.warning("Master node down: %s. Failing over to Slave: %s", url, fallback_url)
                try:
                    g.pupdb_failover = True
                except Exception:
                    pass
                try:
                    return _do_forward(fallback_url, method, headers, data)
                except urllib.error.HTTPError as he:
                    try:
                        err_data = he.read()
                        err_json = json.loads(err_data.decode('utf-8'))
                        return err_json, he.code
                    except Exception:
                        return {'error': he.reason}, he.code
                except Exception as fallback_err:
                    return {'error': 'Both Master and Slave failed. Master error: {}, Slave error: {}'.format(str(e), str(fallback_err))}, 502
        return {'error': 'Router error: {}'.format(str(e))}, 500


@APP.route('/get', methods=['GET'])
def router_get():
    """ Proxy endpoint for PupDB's db_get() method. """
    try:
        key = request.args.get('key')
        if not key:
            return {'error': "Missing parameter 'key'"}, 400

        shards = get_shards()
        if not shards:
            return {'error': 'No shards configured'}, 500

        shard_url = get_shard_url(key, shards)
        url = '{}/get?key={}'.format(shard_url, urllib.parse.quote(key))
        resp, code = forward_request(url, 'GET', request.headers)
        return resp, code
    except Exception as e:
        return {'error': 'Unable to process this request. Details: {}'.format(str(e))}, 422


@APP.route('/set', methods=['POST'])
def router_set():
    """ Proxy endpoint for PupDB's db_set() method. """
    try:
        key = request.json.get('key')
        value = request.json.get('value')
        if not key:
            return {'error': "Missing parameter 'key'"}, 400
        if not value:
            return {'error': "Missing parameter 'value'"}, 400

        shards = get_shards()
        if not shards:
            return {'error': 'No shards configured'}, 500

        shard_url = get_shard_url(key, shards)
        url = '{}/set'.format(shard_url)
        payload = json.dumps({'key': key, 'value': value}).encode('utf-8')
        resp, code = forward_request(url, 'POST', request.headers, payload)
        return resp, code
    except Exception as e:
        return {'error': 'Unable to process this request. Details: {}'.format(str(e))}, 422


@APP.route('/remove/<key>', methods=['DELETE'])
def router_remove(key):
    """ Proxy endpoint for PupDB's db_remove() method. """
    try:
        if not key:
            return {'error': "Missing parameter 'key'"}, 400

        shards = get_shards()
        if not shards:
            return {'error': 'No shards configured'}, 500

        shard_url = get_shard_url(key, shards)
        url = '{}/remove/{}'.format(shard_url, urllib.parse.quote(key))
        resp, code = forward_request(url, 'DELETE', request.headers)
        return resp, code
    except Exception as e:
        return {'error': 'Unable to process this request. Details: {}'.format(str(e))}, 422


@APP.route('/keys', methods=['GET'])
def router_keys():
    """ Proxy endpoint for aggregating keys from all shards. """
    shards = get_shards()
    if not shards:
        return {'error': 'No shards configured'}, 500

    all_keys = []
    for shard in shards:
        resp, code = forward_request('{}/keys'.format(shard), 'GET')
        if code == 200 and isinstance(resp, dict) and 'keys' in resp:
            all_keys.extend(resp['keys'])
    return {'keys': list(set(all_keys))}, 200


@APP.route('/values', methods=['GET'])
def router_values():
    """ Proxy endpoint for aggregating values from all shards. """
    shards = get_shards()
    if not shards:
        return {'error': 'No shards configured'}, 500

    all_values = []
    for shard in shards:
        resp, code = forward_request('{}/values'.format(shard), 'GET')
        if code == 200 and isinstance(resp, dict) and 'values' in resp:
            all_values.extend(resp['values'])
    return {'values': all_values}, 200


@APP.route('/items', methods=['GET'])
def router_items():
    """ Proxy endpoint for aggregating items from all shards. """
    shards = get_shards()
    if not shards:
        return {'error': 'No shards configured'}, 500

    all_items = []
    for shard in shards:
        resp, code = forward_request('{}/items'.format(shard), 'GET')
        if code == 200 and isinstance(resp, dict) and 'items' in resp:
            all_items.extend(resp['items'])
    return {'items': all_items}, 200


@APP.route('/dumps', methods=['GET'])
def router_dumps():
    """ Proxy endpoint for merging database dumps from all shards. """
    shards = get_shards()
    if not shards:
        return {'error': 'No shards configured'}, 500

    merged_db = {}
    for shard in shards:
        resp, code = forward_request('{}/dumps'.format(shard), 'GET')
        if code == 200 and isinstance(resp, dict) and 'database' in resp:
            merged_db.update(resp['database'])
    return {'database': merged_db}, 200


@APP.route('/truncate-db', methods=['POST'])
def router_truncate():
    """ Proxy endpoint for truncating database in all shards. """
    shards = get_shards()
    if not shards:
        return {'error': 'No shards configured'}, 500

    success = True
    errors = []
    for shard in shards:
        resp, code = forward_request('{}/truncate-db'.format(shard), 'POST')
        if code != 200:
            success = False
            errors.append(resp)

    if success:
        return {'message': 'DB cluster has been truncated successfully.'}, 200
    return {'error': 'Failed to truncate one or more shards', 'details': errors}, 400


@APP.route('/recycle-bin', methods=['GET'])
def router_recycle_bin():
    """ Proxy endpoint to aggregate the Recycle Bin from all shards. """
    shards = get_shards()
    if not shards:
        return {'error': 'No shards configured'}, 500

    merged_recycle = {}
    for i, shard in enumerate(shards):
        resp, code = forward_request('{}/recycle-bin'.format(shard), 'GET')
        if code == 200 and isinstance(resp, dict):
            for k, v in resp.items():
                if isinstance(v, dict):
                    merged_recycle[k] = {
                        "value": v.get("value"),
                        "deleted_at": v.get("deleted_at"),
                        "shard": i + 1
                    }
    return jsonify(merged_recycle), 200


@APP.route('/restore', methods=['POST'])
def router_restore():
    """ Proxy endpoint to restore a key from the Recycle Bin of its corresponding shard. """
    try:
        key = request.json.get('key')
        if not key:
            return {'error': "Missing parameter 'key'"}, 400

        shards = get_shards()
        if not shards:
            return {'error': 'No shards configured'}, 500

        shard_url = get_shard_url(key, shards)
        url = '{}/restore'.format(shard_url)
        payload = json.dumps({'key': key}).encode('utf-8')
        resp, code = forward_request(url, 'POST', request.headers, payload)
        return resp, code
    except Exception as e:
        return {'error': 'Unable to process restore request. Details: {}'.format(str(e))}, 422


@APP.after_request
def add_cors_headers(response):
    """ Allow cross-origin requests for dashboard integration. """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, DELETE, OPTIONS'
    
    try:
        if getattr(g, 'pupdb_failover', False):
            response.headers['X-PupDB-Failover'] = 'true'
            response.headers['Access-Control-Expose-Headers'] = 'X-PupDB-Failover'
    except Exception:
        pass
        
    return response
