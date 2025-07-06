import threading
import time
import random
import json
import os
from flask import Flask, request, jsonify
import config

app = Flask(__name__)

class GFSMaster():
    def __init__(self):
        self.files = {}
        self.chunks = {}
        self.chunk_servers = {}
        self.next_chunk_handle = 0
        self.file_to_chunks = {}
        self.chunk_leases = {}
        self.lock = threading.RLock()
        self.op_log_file = config.OPERATION_LOG

        self.load_metadata()

        # Background threads
        threading.Thread(target=self.monitor_chunk_servers, daemon=True).start()
        threading.Thread(target=self.garbage_collection, daemon=True).start()

    def load_metadata(self):
        if os.path.exists(config.METADATA_STORE):
            with open(config.METADATA_STORE, 'r') as f:
                data = json.load(f)
                self.files = data.get('files', {})
                self.chunks = data.get('chunks', {})
                self.file_to_chunks = data.get('file_to_chunks', {})
                self.next_chunk_handle = data.get('next_chunk_handle', 0)

    def save_metadata(self):
        with self.lock:
            with open(config.METADATA_STORE, 'w') as f:
                data = {
                    'files': self.files,
                    'chunks': self.chunks,
                    'file_to_chunks': self.file_to_chunks,
                    'next_chunk_handle': self.next_chunk_handle
                }
                json.dump(data, f)

    def log_operation(self, op, **kwargs):
        with open(self.op_log_file, 'a') as f:
            log_entry = json.dumps({'op': op, 'timestamp': time.time(), **kwargs})
            f.write(log_entry + '\n')

    def register_chunk_server(self, port, data_dir):
        with self.lock:
            server_id = f"{request.remote_addr}:{port}"
            self.chunk_servers[server_id] = {
                'last_heartbeat': time.time(),
                'port': port,
                'data_dir': data_dir,
                'chunks': []
            }
            self.log_operation('register_chunk_server', server_id=server_id, port=port, data_dir=data_dir)
            return server_id

    def handle_heartbeat(self, server_id, chunk_report):
        with self.lock:
            if server_id in self.chunk_servers:
                self.chunk_servers[server_id]['last_heartbeat'] = time.time()
                self.chunk_servers[server_id]['chunks'] = chunk_report
                return {'status': 'ok'}
            else:
                return {'status': 're-register'}

    def create_file(self, filename):
        with self.lock:
            if filename in self.files:
                return None
            self.files[filename] = {'length': 0, 'chunks': {}}
            self.file_to_chunks[filename] = []
            self.log_operation('create_file', filename=filename)
            self.save_metadata()
            return self.files[filename]

    def allocate_chunk(self, filename, chunk_index):
        with self.lock:
            if filename not in self.files:
                return None

            chunk_handle = str(self.next_chunk_handle)
            self.next_chunk_handle += 1

            available_servers = list(self.chunk_servers.keys())
            if len(available_servers) < config.REPLICATION_FACTOR:
                return None

            replicas = random.sample(available_servers, config.REPLICATION_FACTOR)
            self.chunks[chunk_handle] = {'replicas': replicas, 'version': 0}
            self.files[filename]['chunks'][chunk_index] = chunk_handle
            self.file_to_chunks[filename].append(chunk_handle)

            primary_server_id = replicas[0]
            lease_expiry = time.time() + config.LEASE_TIME_SECONDS
            self.chunk_leases[chunk_handle] = (primary_server_id, lease_expiry)

            self.log_operation('allocate_chunk', filename=filename, chunk_index=chunk_index, chunk_handle=chunk_handle, replicas=replicas)
            self.save_metadata()

            return {
                'chunk_handle': chunk_handle,
                'locations': [self.chunk_servers[s]['port'] for s in replicas],
                'primary': self.chunk_servers[primary_server_id]['port']
            }

    def get_chunk_locations(self, filename, chunk_index):
        with self.lock:
            if filename not in self.files or str(chunk_index) not in self.files[filename]['chunks']:
                return None

            chunk_handle = self.files[filename]['chunks'][str(chunk_index)]
            chunk_info = self.chunks.get(chunk_handle)
            if not chunk_info:
                return None

            primary_server_id, lease_expiry = self.chunk_leases.get(chunk_handle, (None, 0))
            if time.time() > lease_expiry:
                primary_server_id = chunk_info['replicas'][0]
                lease_expiry = time.time() + config.LEASE_TIME_SECONDS
                self.chunk_leases[chunk_handle] = (primary_server_id, lease_expiry)

            return {
                'chunk_handle': chunk_handle,
                'locations': [self.chunk_servers[s]['port'] for s in chunk_info['replicas']],
                'primary': self.chunk_servers[primary_server_id]['port']
            }

    def monitor_chunk_servers(self):
        while True:
            time.sleep(config.HEARTBEAT_INTERVAL_SECONDS)
            with self.lock:
                now = time.time()
                dead_servers = []
                for server_id, info in self.chunk_servers.items():
                    if now - info['last_heartbeat'] > config.HEARTBEAT_INTERVAL_SECONDS * 2:
                        dead_servers.append(server_id)
                for server_id in dead_servers:
                    print(f"Chunk server {server_id} is down.")
                    del self.chunk_servers[server_id]
                    self.log_operation('server_down', server_id=server_id)

    def garbage_collection(self):
        # In a real implementation, this would be more sophisticated
        pass

    def get_file_info(self, filename):
        with self.lock:
            if filename in self.files:
                return {'length': self.files[filename]['length']}
            return None

    def update_file_length(self, filename, length):
        with self.lock:
            if filename in self.files:
                self.files[filename]['length'] = length
                self.log_operation('update_file_length', filename=filename, length=length)
                self.save_metadata()
                return True
            return False

master = GFSMaster()

@app.route('/register', methods=['POST'])
def register():
    data = request.json
    server_id = master.register_chunk_server(data['port'], data['data_dir'])
    return jsonify({'server_id': server_id})

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = request.json
    result = master.handle_heartbeat(data['server_id'], data['chunk_report'])
    return jsonify(result)

@app.route('/create', methods=['POST'])
def create():
    print("--- Received create request ---")
    filename = request.json['filename']
    print(f"--- Creating file: {filename} ---")
    if master.create_file(filename) is not None:
        print(f"--- File {filename} created successfully ---")
        return jsonify({'status': 'created'})
    else:
        print(f"--- File {filename} already exists ---")
        return jsonify({'error': 'file_exists'}), 409

@app.route('/get_chunk_locations', methods=['GET'])
def get_chunk_locations():
    filename = request.args['filename']
    chunk_index = str(request.args['chunk_index'])
    locations = master.get_chunk_locations(filename, chunk_index)
    if locations:
        return jsonify(locations)
    else:
        # If chunk doesn't exist, allocate it
        new_chunk_info = master.allocate_chunk(filename, chunk_index)
        if new_chunk_info:
            return jsonify(new_chunk_info)
        else:
            return jsonify({'error': 'cannot_allocate_chunk'}), 500

@app.route('/ls', methods=['GET'])
def ls():
    path = request.args.get('path', '/')
    print(f"--- Received ls request for path: {path} ---")
    all_files = list(master.files.keys())
    print(f"--- All files on master: {all_files} ---")
    if path == '/':
        return jsonify(all_files)
    else:
        # This is a simple implementation. A real implementation would handle directories.
        filtered_files = [f for f in all_files if f.startswith(path)]
        print(f"--- Filtered files: {filtered_files} ---")
        return jsonify(filtered_files)

@app.route('/get_file_info', methods=['GET'])
def get_file_info():
    filename = request.args['filename']
    info = master.get_file_info(filename)
    if info:
        return jsonify(info)
    else:
        return jsonify({'error': 'file_not_found'}), 404

@app.route('/update_file_length', methods=['POST'])
def update_file_length():
    filename = request.json['filename']
    length = request.json['length']
    if master.update_file_length(filename, length):
        return jsonify({'status': 'updated'})
    else:
        return jsonify({'error': 'file_not_found'}), 404

if __name__ == '__main__':
    print(f"--- Starting master server on {config.MASTER_HOST}:{config.MASTER_PORT} ---")
    app.run(host=config.MASTER_HOST, port=config.MASTER_PORT, debug=True)