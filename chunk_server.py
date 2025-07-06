import os
import sys
import time
import requests
import threading
import json
from flask import Flask, request, jsonify
import config

app = Flask(__name__)

class GFSChunkServer:
    def __init__(self, port, data_dir):
        self.port = port
        self.data_dir = data_dir
        self.server_id = None
        self.master_url = f"http://{config.MASTER_HOST}:{config.MASTER_PORT}"
        self.chunks = {}
        self.lock = threading.Lock()
        self.op_queue = []
        self.processed_requests = set()

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        self.load_metadata()

        threading.Thread(target=self.register_with_master, daemon=True).start()
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        threading.Thread(target=self.process_op_queue, daemon=True).start()

    def load_metadata(self):
        metadata_path = os.path.join(self.data_dir, 'chunk_metadata.json')
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                self.chunks = json.load(f)

    def save_metadata(self):
        with self.lock:
            metadata_path = os.path.join(self.data_dir, 'chunk_metadata.json')
            with open(metadata_path, 'w') as f:
                json.dump(self.chunks, f)

    def register_with_master(self):
        while self.server_id is None:
            try:
                response = requests.post(f"{self.master_url}/register", json={
                    'port': self.port,
                    'data_dir': self.data_dir
                })
                if response.status_code == 200:
                    self.server_id = response.json()['server_id']
                    print(f"Registered with master. Server ID: {self.server_id}")
                else:
                    time.sleep(5)
            except requests.exceptions.ConnectionError:
                time.sleep(5)

    def send_heartbeat(self):
        while True:
            if self.server_id:
                try:
                    chunk_report = list(self.chunks.keys())
                    requests.post(f"{self.master_url}/heartbeat", json={
                        'server_id': self.server_id,
                        'chunk_report': chunk_report
                    })
                except requests.exceptions.ConnectionError:
                    print("Master not available.")
            time.sleep(config.HEARTBEAT_INTERVAL_SECONDS)

    def process_op_queue(self):
        while True:
            if self.op_queue:
                op = self.op_queue.pop(0)
                if op['type'] == 'write':
                    self._handle_write(op['data'])
                elif op['type'] == 'append':
                    self._handle_append(op['data'])
            else:
                time.sleep(0.1)

    def queue_operation(self, op_type, data):
        self.op_queue.append({'type': op_type, 'data': data})

    def _handle_write(self, data):
        chunk_handle = str(data['chunk_handle'])
        chunk_data = data['data']
        chunk_offset = data.get('offset', 0)
        chunk_path = os.path.join(self.data_dir, chunk_handle)

        mode = 'r+b' if os.path.exists(chunk_path) else 'wb'
        with open(chunk_path, mode) as f:
            if mode == 'r+b':
                f.seek(chunk_offset)
            f.write(chunk_data.encode('utf-8'))
        self.chunks[chunk_handle] = {'version': data.get('version', 1)}
        self.save_metadata()

    def _handle_append(self, data):
        request_id = data['request_id']
        if request_id in self.processed_requests:
            return

        chunk_handle = data['chunk_handle']
        chunk_data = data['data']
        chunk_path = os.path.join(self.data_dir, str(chunk_handle))
        with open(chunk_path, 'a') as f:
            f.write(chunk_data)
        self.chunks[chunk_handle] = {'version': data.get('version', 1)}
        self.processed_requests.add(request_id)
        self.save_metadata()

    def read_chunk(self, chunk_handle):
        chunk_path = os.path.join(self.data_dir, str(chunk_handle))
        if os.path.exists(chunk_path):
            with open(chunk_path, 'rb') as f:
                return f.read().decode('utf-8')
        return None

chunk_server = None

@app.route('/write', methods=['POST'])
def write():
    data = request.json
    chunk_server.queue_operation('write', data)
    return jsonify({'status': 'write_queued'})

@app.route('/append', methods=['POST'])
def append():
    data = request.json
    chunk_server.queue_operation('append', data)
    return jsonify({'status': 'append_queued'})

@app.route('/read', methods=['GET'])
def read():
    chunk_handle = str(request.args['chunk_handle'])
    content = chunk_server.read_chunk(chunk_handle)
    if content is not None:
        return jsonify({'data': content})
    else:
        return jsonify({'data': ''})

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python chunk_server.py <port> <data_directory>")
        sys.exit(1)

    port = int(sys.argv[1])
    data_dir = sys.argv[2]
    chunk_server = GFSChunkServer(port, data_dir)
    app.run(port=port, debug=True, use_reloader=False)