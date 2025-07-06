import requests
import uuid
import time
import config

class GFSClient:
    def __init__(self):
        self.master_url = f"http://{config.MASTER_HOST}:{config.MASTER_PORT}"
        self.chunk_cache = {}

    def _get_chunk_locations(self, filename, chunk_index):
        cache_key = f"{filename}:{chunk_index}"
        if cache_key in self.chunk_cache and time.time() < self.chunk_cache[cache_key]['expiry']:
            return self.chunk_cache[cache_key]['locations']

        try:
            response = requests.get(f"{self.master_url}/get_chunk_locations", params={
                'filename': filename,
                'chunk_index': chunk_index
            })
            if response.status_code == 200:
                locations = response.json()
                self.chunk_cache[cache_key] = {
                    'locations': locations,
                    'expiry': time.time() + config.CLIENT_CHUNK_CACHE_TTL_SECONDS
                }
                return locations
            else:
                return None
        except requests.exceptions.ConnectionError:
            return None

    def create(self, filename):
        try:
            response = requests.post(f"{self.master_url}/create", json={'filename': filename}, timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return False

    def ls(self, path):
        try:
            response = requests.get(f"{self.master_url}/ls", params={'path': path})
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except requests.exceptions.ConnectionError:
            return None

    def get_file_info(self, filename):
        try:
            response = requests.get(f"{self.master_url}/get_file_info", params={'filename': filename}, timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while getting file info: {e}")
            return None

    def write(self, filename, data, offset=0):
        chunk_index = offset // config.CHUNK_SIZE_BYTES
        chunk_offset = offset % config.CHUNK_SIZE_BYTES

        locations = self._get_chunk_locations(filename, chunk_index)
        if not locations:
            return False

        chunk_handle = locations['chunk_handle']
        primary_port = locations['primary']
        replica_ports = locations['locations']

        # In a real implementation, we would write to the primary and the primary would forward to secondaries.
        # For simplicity, we write to all replicas directly.
        for port in replica_ports:
            try:
                requests.post(f"http://127.0.0.1:{port}/write", json={
                    'chunk_handle': chunk_handle,
                    'data': data,
                    'offset': chunk_offset
                })
            except requests.exceptions.ConnectionError:
                continue
        return True

    def append(self, filename, data):
        file_info = self.get_file_info(filename)
        if not file_info:
            print(f"Error: Could not get file info for {filename}")
            return False

        current_length = file_info.get('length', 0)
        if self.write(filename, data, offset=current_length):
            # After successful write, inform master about new length
            new_length = current_length + len(data)
            return self.update_file_length(filename, new_length)
        return False

    def update_file_length(self, filename, new_length):
        try:
            response = requests.post(f"{self.master_url}/update_file_length", json={'filename': filename, 'length': new_length}, timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while updating file length: {e}")
            return False

    def read(self, filename, offset=0, length=-1):
        chunk_index = offset // config.CHUNK_SIZE_BYTES
        chunk_offset = offset % config.CHUNK_SIZE_BYTES

        locations = self._get_chunk_locations(filename, chunk_index)
        if not locations:
            return None

        chunk_handle = locations['chunk_handle']
        replica_ports = locations['locations']

        for port in replica_ports:
            try:
                response = requests.get(f"http://127.0.0.1:{port}/read", params={'chunk_handle': chunk_handle})
                if response.status_code == 200:
                    # In a real implementation, we would handle chunk boundaries and length
                    return response.json()['data']
            except requests.exceptions.ConnectionError:
                continue
        return None

if __name__ == '__main__':
    client = GFSClient()

    while True:
        print("\nAvailable commands: create, write, append, read, ls, exit")
        command = input("Enter command: ").strip().lower()

        if command == 'exit':
            break

        if command == 'create':
            filename = input("Enter filename to create: ").strip()
            if client.create(filename):
                print(f"File '{filename}' created successfully.")
            else:
                print(f"Failed to create file '{filename}'.")

        elif command == 'write':
            filename = input("Enter filename to write to: ").strip()
            data = input("Enter data to write: ")
            if client.write(filename, data):
                print("Write successful.")
            else:
                print("Write failed.")

        elif command == 'append':
            filename = input("Enter filename to append to: ").strip()
            data = input("Enter data to append: ")
            if client.append(filename, data):
                print("Append successful.")
            else:
                print("Append failed.")

        elif command == 'read':
            filename = input("Enter filename to read from: ").strip()
            content = client.read(filename)
            if content is not None:
                print(f"File content: {content}")
            else:
                print("Read failed.")

        elif command == 'ls':
            path = input("Enter path to list (default '/'): ").strip()
            if not path:
                path = '/'
            files = client.ls(path)
            if files is not None:
                print(f"Files in '{path}':")
                for f in files:
                    print(f)
            else:
                print("Failed to list files.")

        else:
            print("Unknown command.")