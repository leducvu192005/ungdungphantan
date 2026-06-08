"""
    Integration tests for distributed features in PupDB.
"""

import sys
import os
# Ensure parent directory is in sys.path so spawned processes can resolve 'pupdb'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import time
import socket
import urllib.request
import urllib.error
import multiprocessing
import logging

# pyrefly: ignore [missing-import]
import pytest

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(process)d | %(levelname)s | %(message)s'
)


def wait_for_port(port, timeout=5.0):
    """ Helper function to wait for a local port to become active. """
    start_time = time.time()
    while True:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.5):
                return True
        except OSError:
            if time.time() - start_time > timeout:
                raise RuntimeError("Server on port {} did not start in {} seconds".format(port, timeout))
            time.sleep(0.1)


def run_master_server(db_path, port, slave_url):
    """ Target to run Master PupDB server in a separate process. """
    os.environ['PUPDB_FILE_PATH'] = db_path
    os.environ['PUPDB_ROLE'] = 'master'
    os.environ['PUPDB_SLAVE_URL'] = slave_url

    # Suppress flask logs to keep test output clean
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    from pupdb.rest import APP, DB
    DB.init_db()
    APP.run(port=port, debug=False, use_reloader=False)


def run_slave_server(db_path, port):
    """ Target to run Slave PupDB server in a separate process. """
    os.environ['PUPDB_FILE_PATH'] = db_path
    os.environ['PUPDB_ROLE'] = 'slave'
    os.environ['PUPDB_SLAVE_URL'] = ''

    # Suppress flask logs to keep test output clean
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    from pupdb.rest import APP, DB
    DB.init_db()
    APP.run(port=port, debug=False, use_reloader=False)


def run_router_server(port, shards):
    """ Target to run Router proxy server in a separate process. """
    os.environ['PUPDB_SHARDS'] = ','.join(shards)

    # Suppress flask logs to keep test output clean
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    from pupdb.router import APP
    APP.run(port=port, debug=False, use_reloader=False)


def run_router_server_with_slaves(port, shards, slaves):
    """ Target to run Router proxy server with configured slaves. """
    os.environ['PUPDB_SHARDS'] = ','.join(shards)
    os.environ['PUPDB_SLAVES'] = ','.join(slaves)

    # Suppress flask logs to keep test output clean
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    from pupdb.router import APP
    APP.run(port=port, debug=False, use_reloader=False)


def make_http_request(url, method='GET', data=None):
    """ Helper to make synchronous HTTP requests to our test servers. """
    req = urllib.request.Request(url, method=method)
    if data is not None:
        req.add_header('Content-Type', 'application/json')
        req.data = json.dumps(data).encode('utf-8')

    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            status = response.status
            body = response.read().decode('utf-8')
            try:
                return json.loads(body), status
            except Exception:
                return body, status
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')
        try:
            return json.loads(body), e.code
        except Exception:
            return body, e.code


def clean_db_files(*paths):
    """ Helper to delete database and lock files. """
    for path in paths:
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass
        lock_path = '{}.lock'.format(path)
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
            except OSError:
                pass


def test_active_passive_replication():
    """ Tests that sets/removes on Master are asynchronously replicated to Slave. """
    master_db = 'master_db.json'
    slave_db = 'slave_db.json'
    master_port = 25001
    slave_port = 25002

    clean_db_files(master_db, slave_db)

    # Start Slave Node
    slave_proc = multiprocessing.Process(
        target=run_slave_server,
        args=(slave_db, slave_port)
    )
    slave_proc.daemon = True
    slave_proc.start()

    # Start Master Node
    master_proc = multiprocessing.Process(
        target=run_master_server,
        args=(master_db, master_port, 'http://127.0.0.1:{}'.format(slave_port))
    )
    master_proc.daemon = True
    master_proc.start()

    try:
        # Wait for both servers to be ready
        wait_for_port(slave_port)
        wait_for_port(master_port)

        # 1. Set key on Master
        res, status = make_http_request(
            'http://127.0.0.1:{}/set'.format(master_port),
            'POST',
            {'key': 'rep_test', 'value': 'hello_replica'}
        )
        assert status == 200

        # Wait a small duration for thread to replicate the request
        time.sleep(0.3)

        # Verify key exists on Master
        res, status = make_http_request('http://127.0.0.1:{}/get?key=rep_test'.format(master_port))
        assert status == 200
        assert res['value'] == 'hello_replica'

        # Verify key has replicated to Slave
        res, status = make_http_request('http://127.0.0.1:{}/get?key=rep_test'.format(slave_port))
        assert status == 200
        assert res['value'] == 'hello_replica'

        # 2. Remove key on Master
        res, status = make_http_request(
            'http://127.0.0.1:{}/remove/rep_test'.format(master_port),
            'DELETE'
        )
        assert status == 200

        # Wait a small duration for thread to replicate the deletion
        time.sleep(0.3)

        # Verify key is gone from Master
        res, status = make_http_request('http://127.0.0.1:{}/get?key=rep_test'.format(master_port))
        assert status == 200
        assert res['value'] is None

        # Verify key is gone from Slave
        res, status = make_http_request('http://127.0.0.1:{}/get?key=rep_test'.format(slave_port))
        assert status == 200
        assert res['value'] is None

        # 3. Truncate Master and verify it replicates to Slave
        # First write a key so we can test clearing it
        res, status = make_http_request(
            'http://127.0.0.1:{}/set'.format(master_port),
            'POST',
            {'key': 'rep_test2', 'value': 'hello_replica2'}
        )
        assert status == 200
        time.sleep(0.3)

        # Verify key exists on both Master and Slave
        res, status = make_http_request('http://127.0.0.1:{}/get?key=rep_test2'.format(master_port))
        assert res['value'] == 'hello_replica2'
        res, status = make_http_request('http://127.0.0.1:{}/get?key=rep_test2'.format(slave_port))
        assert res['value'] == 'hello_replica2'

        # Call truncate-db on Master
        res, status = make_http_request(
            'http://127.0.0.1:{}/truncate-db'.format(master_port),
            'POST'
        )
        assert status == 200
        time.sleep(0.3)

        # Verify both Master and Slave are completely truncated/empty
        res, status = make_http_request('http://127.0.0.1:{}/dumps'.format(master_port))
        assert res['database'] == {}
        res, status = make_http_request('http://127.0.0.1:{}/dumps'.format(slave_port))
        assert res['database'] == {}

    finally:
        # Clean up processes and files
        master_proc.terminate()
        slave_proc.terminate()
        master_proc.join()
        slave_proc.join()
        clean_db_files(master_db, slave_db)


def test_distributed_sharding_router():
    """ Tests sharding routing based on first-character ASCII hashing and global aggregate views. """
    shard1_db = 'shard1_db.json'
    shard2_db = 'shard2_db.json'
    shard1_port = 26001
    shard2_port = 26002
    router_port = 26000

    clean_db_files(shard1_db, shard2_db)

    # Start Shard 1
    s1_proc = multiprocessing.Process(
        target=run_slave_server,
        args=(shard1_db, shard1_port)
    )
    s1_proc.daemon = True
    s1_proc.start()

    # Start Shard 2
    s2_proc = multiprocessing.Process(
        target=run_slave_server,
        args=(shard2_db, shard2_port)
    )
    s2_proc.daemon = True
    s2_proc.start()

    # Start Router
    shards_list = ['http://127.0.0.1:{}'.format(shard1_port), 'http://127.0.0.1:{}'.format(shard2_port)]
    router_proc = multiprocessing.Process(
        target=run_router_server,
        args=(router_port, shards_list)
    )
    router_proc.daemon = True
    router_proc.start()

    try:
        # Wait for all nodes to be ready
        wait_for_port(shard1_port)
        wait_for_port(shard2_port)
        wait_for_port(router_port)

        # Determine shard indexing: sum(ord(c) for c in key) % 2
        # Key 'B_key': 66 + 95 + 107 + 101 + 121 = 490 -> 490 % 2 = 0 -> Shard 1 (26001)
        # Key 'A_key': 65 + 95 + 107 + 101 + 121 = 489 -> 489 % 2 = 1 -> Shard 2 (26002)

        # 1. Set key 'B' on Router
        res, status = make_http_request(
            'http://127.0.0.1:{}/set'.format(router_port),
            'POST',
            {'key': 'B_key', 'value': 'val_B'}
        )
        assert status == 200

        # Verify routed to Shard 1
        res, status = make_http_request('http://127.0.0.1:{}/get?key=B_key'.format(shard1_port))
        assert status == 200
        assert res['value'] == 'val_B'

        # Verify not present in Shard 2
        res, status = make_http_request('http://127.0.0.1:{}/get?key=B_key'.format(shard2_port))
        assert status == 200
        assert res['value'] is None

        # 2. Set key 'A' on Router
        res, status = make_http_request(
            'http://127.0.0.1:{}/set'.format(router_port),
            'POST',
            {'key': 'A_key', 'value': 'val_A'}
        )
        assert status == 200

        # Verify routed to Shard 2
        res, status = make_http_request('http://127.0.0.1:{}/get?key=A_key'.format(shard2_port))
        assert status == 200
        assert res['value'] == 'val_A'

        # Verify not present in Shard 1
        res, status = make_http_request('http://127.0.0.1:{}/get?key=A_key'.format(shard1_port))
        assert status == 200
        assert res['value'] is None

        # 3. Retrieve keys via Router
        res, status = make_http_request('http://127.0.0.1:{}/get?key=B_key'.format(router_port))
        assert status == 200
        assert res['value'] == 'val_B'

        res, status = make_http_request('http://127.0.0.1:{}/get?key=A_key'.format(router_port))
        assert status == 200
        assert res['value'] == 'val_A'

        # 4. Global endpoints aggregation
        # /keys
        res, status = make_http_request('http://127.0.0.1:{}/keys'.format(router_port))
        assert status == 200
        assert set(res['keys']) == {'A_key', 'B_key'}

        # /values
        res, status = make_http_request('http://127.0.0.1:{}/values'.format(router_port))
        assert status == 200
        assert set(res['values']) == {'val_A', 'val_B'}

        # /items
        res, status = make_http_request('http://127.0.0.1:{}/items'.format(router_port))
        assert status == 200
        items_set = {tuple(x) for x in res['items']}
        assert items_set == {('A_key', 'val_A'), ('B_key', 'val_B')}

        # /dumps
        res, status = make_http_request('http://127.0.0.1:{}/dumps'.format(router_port))
        assert status == 200
        assert res['database'] == {'A_key': 'val_A', 'B_key': 'val_B'}

        # 5. Remove via Router
        res, status = make_http_request(
            'http://127.0.0.1:{}/remove/A_key'.format(router_port),
            'DELETE'
        )
        assert status == 200

        # Verify removed from Shard 2
        res, status = make_http_request('http://127.0.0.1:{}/get?key=A_key'.format(shard2_port))
        assert status == 200
        assert res['value'] is None

        # Verify global keys list
        res, status = make_http_request('http://127.0.0.1:{}/keys'.format(router_port))
        assert status == 200
        assert res['keys'] == ['B_key']

        # 6. Global Truncate
        res, status = make_http_request('http://127.0.0.1:{}/truncate-db'.format(router_port), 'POST')
        assert status == 200

        # Verify all shards empty
        res, status = make_http_request('http://127.0.0.1:{}/dumps'.format(router_port))
        assert status == 200
        assert res['database'] == {}

    finally:
        # Clean up processes and files
        router_proc.terminate()
        s1_proc.terminate()
        s2_proc.terminate()
        router_proc.join()
        s1_proc.join()
        s2_proc.join()
        clean_db_files(shard1_db, shard2_db)


def test_failover_and_failback_sync_back():
    """ Tests that the Router automatically fails over to the Slave when Master is down, and Master syncs back upon restart. """
    master_db = 'master_failover_db.json'
    slave_db = 'slave_failover_db.json'

    master_port = 27001
    slave_port = 27011
    router_port = 27000

    clean_db_files(master_db, slave_db)

    # Start Slave Node
    slave_proc = multiprocessing.Process(
        target=run_slave_server,
        args=(slave_db, slave_port)
    )
    slave_proc.daemon = True
    slave_proc.start()

    # Start Master Node
    master_proc = multiprocessing.Process(
        target=run_master_server,
        args=(master_db, master_port, 'http://127.0.0.1:{}'.format(slave_port))
    )
    master_proc.daemon = True
    master_proc.start()

    # Start Router
    shards_list = ['http://127.0.0.1:{}'.format(master_port)]
    slaves_list = ['http://127.0.0.1:{}'.format(slave_port)]
    router_proc = multiprocessing.Process(
        target=run_router_server_with_slaves,
        args=(router_port, shards_list, slaves_list)
    )
    router_proc.daemon = True
    router_proc.start()

    try:
        # Wait for all nodes to be ready
        wait_for_port(slave_port)
        wait_for_port(master_port)
        wait_for_port(router_port)

        # 1. Normal state: write via Router
        res, status = make_http_request(
            'http://127.0.0.1:{}/set'.format(router_port),
            'POST',
            {'key': 'k1', 'value': 'v1'}
        )
        assert status == 200

        # Wait for replication
        time.sleep(0.3)

        # Verify key exists on both Master and Slave
        res, status = make_http_request('http://127.0.0.1:{}/get?key=k1'.format(master_port))
        assert res['value'] == 'v1'
        res, status = make_http_request('http://127.0.0.1:{}/get?key=k1'.format(slave_port))
        assert res['value'] == 'v1'

        # 2. Simulate Master crash
        master_proc.terminate()
        master_proc.join()

        # 3. Write via Router while Master is down (Failover to Slave)
        res, status = make_http_request(
            'http://127.0.0.1:{}/set'.format(router_port),
            'POST',
            {'key': 'k2', 'value': 'v2_written_to_slave'}
        )
        assert status == 200

        # Read via Router while Master is down (Failover to Slave)
        res, status = make_http_request('http://127.0.0.1:{}/get?key=k2'.format(router_port))
        assert status == 200
        assert res['value'] == 'v2_written_to_slave'

        # Verify it went directly to Slave
        res, status = make_http_request('http://127.0.0.1:{}/get?key=k2'.format(slave_port))
        assert res['value'] == 'v2_written_to_slave'

        # 4. Restart Master Node (Failback & Sync-Back)
        master_proc = multiprocessing.Process(
            target=run_master_server,
            args=(master_db, master_port, 'http://127.0.0.1:{}'.format(slave_port))
        )
        master_proc.daemon = True
        master_proc.start()
        wait_for_port(master_port)

        # Wait for startup synchronization to execute
        time.sleep(0.5)

        # Verify Master has synced the new key (k2) from Slave on startup
        res, status = make_http_request('http://127.0.0.1:{}/get?key=k2'.format(master_port))
        assert status == 200
        assert res['value'] == 'v2_written_to_slave'

        # 5. Read via Router again: should route to Master and find k2
        res, status = make_http_request('http://127.0.0.1:{}/get?key=k2'.format(router_port))
        assert status == 200
        assert res['value'] == 'v2_written_to_slave'

    finally:
        router_proc.terminate()
        if master_proc.is_alive():
            master_proc.terminate()
            master_proc.join()
        slave_proc.terminate()
        router_proc.join()
        slave_proc.join()
        clean_db_files(master_db, slave_db)


def test_recycle_bin_and_restore():
    """ Tests that deleted keys go to Slave's recycle bin and can be restored, isolated from startup sync. """
    master_db = 'master_recycle_db.json'
    slave_db = 'slave_recycle_db.json'
    slave_recycle = '{}_recycle.json'.format(slave_db)

    master_port = 28001
    slave_port = 28011
    router_port = 28000

    clean_db_files(master_db, slave_db, slave_recycle)

    # Start Slave Node
    slave_proc = multiprocessing.Process(
        target=run_slave_server,
        args=(slave_db, slave_port)
    )
    slave_proc.daemon = True
    slave_proc.start()

    # Start Master Node
    master_proc = multiprocessing.Process(
        target=run_master_server,
        args=(master_db, master_port, 'http://127.0.0.1:{}'.format(slave_port))
    )
    master_proc.daemon = True
    master_proc.start()

    # Start Router
    shards_list = ['http://127.0.0.1:{}'.format(master_port)]
    slaves_list = ['http://127.0.0.1:{}'.format(slave_port)]
    router_proc = multiprocessing.Process(
        target=run_router_server_with_slaves,
        args=(router_port, shards_list, slaves_list)
    )
    router_proc.daemon = True
    router_proc.start()

    try:
        # Wait for all nodes to be ready
        wait_for_port(slave_port)
        wait_for_port(master_port)
        wait_for_port(router_port)

        # 1. Set a key via Router
        res, status = make_http_request(
            'http://127.0.0.1:{}/set'.format(router_port),
            'POST',
            {'key': 'item1', 'value': 'value1'}
        )
        assert status == 200
        time.sleep(0.3)

        # Verify key exists on both Master and Slave
        res, status = make_http_request('http://127.0.0.1:{}/get?key=item1'.format(master_port))
        assert res['value'] == 'value1'
        res, status = make_http_request('http://127.0.0.1:{}/get?key=item1'.format(slave_port))
        assert res['value'] == 'value1'

        # 2. Delete key via Router
        res, status = make_http_request(
            'http://127.0.0.1:{}/remove/item1'.format(router_port),
            'DELETE'
        )
        assert status == 200
        time.sleep(0.3)

        # Verify key is deleted from Master and Slave active DB
        res, status = make_http_request('http://127.0.0.1:{}/get?key=item1'.format(master_port))
        assert res['value'] is None
        res, status = make_http_request('http://127.0.0.1:{}/get?key=item1'.format(slave_port))
        assert res['value'] is None

        # Verify key exists in Slave's recycle bin
        res, status = make_http_request('http://127.0.0.1:{}/recycle-bin'.format(router_port))
        assert status == 200
        assert 'item1' in res
        assert res['item1']['value'] == 'value1'

        # 3. Restart Master Node. Verify Master remains empty (does NOT restore deleted key on startup)
        master_proc.terminate()
        master_proc.join()

        master_proc = multiprocessing.Process(
            target=run_master_server,
            args=(master_db, master_port, 'http://127.0.0.1:{}'.format(slave_port))
        )
        master_proc.daemon = True
        master_proc.start()
        wait_for_port(master_port)
        time.sleep(0.5)

        # Verify Master is empty
        res, status = make_http_request('http://127.0.0.1:{}/get?key=item1'.format(master_port))
        assert res['value'] is None

        # 4. Explicitly restore key via Router
        res, status = make_http_request(
            'http://127.0.0.1:{}/restore'.format(router_port),
            'POST',
            {'key': 'item1'}
        )
        assert status == 200
        time.sleep(0.3)

        # Verify key is restored in both Master and Slave active DB
        res, status = make_http_request('http://127.0.0.1:{}/get?key=item1'.format(master_port))
        assert res['value'] == 'value1'
        res, status = make_http_request('http://127.0.0.1:{}/get?key=item1'.format(slave_port))
        assert res['value'] == 'value1'

        # Verify key is gone from recycle bin
        res, status = make_http_request('http://127.0.0.1:{}/recycle-bin'.format(router_port))
        assert 'item1' not in res

    finally:
        router_proc.terminate()
        if master_proc.is_alive():
            master_proc.terminate()
            master_proc.join()
        slave_proc.terminate()
        router_proc.join()
        slave_proc.join()
        clean_db_files(master_db, slave_db, slave_recycle)
