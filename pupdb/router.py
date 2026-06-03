"""
    This module represents the distributed sharding router proxy for PupDB.
"""

import os
import json
import urllib.request
import urllib.parse
import urllib.error
import logging

from flask import Flask, request, Response, jsonify

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
    app = Flask(__name__)
    app.response_class = CustomResponse
    return app


APP = init_module()


def get_shards():
    """ Returns the list of configured shard URLs. """
    shards_env = os.environ.get('PUPDB_SHARDS')
    if shards_env:
        return [s.strip() for s in shards_env.split(',') if s.strip()]
    # Default fallback for testing or standalone setup
    return ['http://127.0.0.1:4001', 'http://127.0.0.1:4002']


def get_shard_url(key, shards):
    """ Hashes the key using its first character's ASCII value to select a shard. """
    if not key or not shards:
        return None
    first_char = str(key)[0]
    index = ord(first_char) % len(shards)
    return shards[index]


def forward_request(url, method, headers=None, data=None):
    """ Forwards the HTTP request to the selected shard and returns (response, status_code). """
    try:
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
    except urllib.error.HTTPError as e:
        try:
            err_data = e.read()
            err_json = json.loads(err_data.decode('utf-8'))
            return err_json, e.code
        except Exception:
            return {'error': e.reason}, e.code
    except Exception as e:
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
