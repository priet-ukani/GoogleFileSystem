import socket
import threading
import time
import pickle
import uuid

# Constants for the server
CLIENT_HOST = '127.0.0.1'
CLIENT_PORT = 5003  # Port for client connections
CHUNK_HOST = '127.0.0.1'
CHUNK_PORT = 5004     # Port for chunk server connections
CHUNK_SIZE = 1024
MASTER_HOST = '127.0.0.1'

# Globals
available_chunks = set()
mapping_counter = 0

# Port mapping and leftover data
chunk_server_port_for_client = {}
mapping_for_finding_file = {}
leftover_chunk_data = {}  # Store leftover chunk data by unique chunk ID

class AppendTransaction:
    def __init__(self, filename, data):
        self.transaction_id = str(uuid.uuid4())
        self.filename = filename
        self.data = data
        self.status = 'INIT'
        self.prepared_servers = set()
        self.committed_servers = set()
        self.timestamp = time.time()

class ExactlyOnceAppendProtocol:
    def __init__(self):
        self.active_transactions = {}
        self.completed_transactions = {}
        self.transaction_timeout = 60  # 1-minute timeout

    def create_append_transaction(self, filename, data):
        transaction = AppendTransaction(filename, data)
        self.active_transactions[transaction.transaction_id] = transaction
        return transaction

    def validate_transaction(self, transaction_id):
        # Check if transaction has already been committed
        if transaction_id in self.completed_transactions:
            return False

        # Check for timeout
        transaction = self.active_transactions.get(transaction_id)
        if transaction and time.time() - transaction.timestamp > self.transaction_timeout:
            self.abort_transaction(transaction_id)
            return False

        return True

    def record_prepared_server(self, transaction_id, server_address):
        transaction = self.active_transactions.get(transaction_id)
        if transaction:
            transaction.prepared_servers.add(server_address)

    def record_committed_server(self, transaction_id, server_address):
        transaction = self.active_transactions.get(transaction_id)
        if transaction:
            transaction.committed_servers.add(server_address)
            
            # If all prepared servers have committed, finalize transaction
            if transaction.prepared_servers == transaction.committed_servers:
                self.completed_transactions[transaction_id] = transaction
                del self.active_transactions[transaction_id]

    def abort_transaction(self, transaction_id):
        if transaction_id in self.active_transactions:
            del self.active_transactions[transaction_id]

# Integrate with existing append handling in master server
exactly_once_append_protocol = ExactlyOnceAppendProtocol()

def handle_append_transaction(client_socket, filename, data):
    # Create a new transaction
    transaction = exactly_once_append_protocol.create_append_transaction(filename, data)

    # Find the last chunk for this file
    last_chunk_index = determine_last_chunk_index(filename)
    
    # Get chunk servers for the last chunk
    chunk_mapping = get_chunk_mapping(filename, last_chunk_index)
    chunk_servers = chunk_mapping["servers"]

    # Prepare response for client
    response = {
        "transaction_id": transaction.transaction_id,
        "last_chunk_index": last_chunk_index,
        "chunk_servers": chunk_servers
    }

    send_response(client_socket, response)

def send_request(socket, request):
    data = pickle.dumps(request)
    socket.sendall(len(data).to_bytes(4, byteorder='big'))
    socket.sendall(data)

def receive_response(socket):
    length_data = socket.recv(4)
    length = int.from_bytes(length_data, byteorder='big')
    data = b''
    while len(data) < length:
        data += socket.recv(length - len(data))
    return pickle.loads(data)

def send_response(client_socket, response):
    response_data = pickle.dumps(response)
    client_socket.sendall(len(response_data).to_bytes(4, byteorder='big'))
    client_socket.sendall(response_data)


# Function to handle client connections
def handle_client(client_socket, address):
    global mapping_counter
    while True:
        try:
            # Receive the length of the incoming data (4 bytes)
            length_data = client_socket.recv(4)
            if not length_data:
                print(f"Client {address} disconnected.")
                break
            
            length = int.from_bytes(length_data, byteorder='big')
            data = b''
            while len(data) < length:
                data += client_socket.recv(length - len(data))

            # Deserialize the data using pickle
            jsonobject = pickle.loads(data)
            print(f"Received request from client {address}: {jsonobject}")
            
            if jsonobject['operation'] == 'upload':
                # Create mappings for each chunk of the file
                toretarr = []
                last_chunk_data = CHUNK_SIZE - jsonobject['last_chunk_size']  # Size of leftover data in the last chunk
                
                for i in range(mapping_counter, mapping_counter + jsonobject['num_chunks']):
                    chunk_servers = list(available_chunks)
                    if len(chunk_servers) < 2:
                        print("Not enough chunk servers available")
                        break
                    
                    # Select two chunk servers for replication
                    chunk_server1, chunk_server2 = chunk_servers[:2]
                    
                    # Map filename and chunk number to unique chunk ID and chunk servers
                    unique_chunk_id = i
                    chunk_mapping = {
                        "unique_id": unique_chunk_id,
                        "servers": [chunk_server_port_for_client[chunk_server1], chunk_server_port_for_client[chunk_server2]]
                    }
                    mapping_for_finding_file[(jsonobject["filename"], i - mapping_counter)] = chunk_mapping
                    
                    # Store leftover data if this is the last chunk
                    if i == mapping_counter + jsonobject['num_chunks'] - 1:
                        leftover_chunk_data[unique_chunk_id] = last_chunk_data
                    
                    # Append to response array for the client
                    toretarr.append([
                        unique_chunk_id,
                        chunk_server_port_for_client[chunk_server1],
                        chunk_server_port_for_client[chunk_server2]
                    ])
                
                mapping_counter += jsonobject['num_chunks']
                response = {"mapping": toretarr}
                
                # Serialize the response and send it back to the client
                response_data = pickle.dumps(response)
                client_socket.sendall(len(response_data).to_bytes(4, byteorder='big'))
                client_socket.sendall(response_data)
                print(f"Updated mappings: {mapping_for_finding_file}")
                print(f"Stored leftover data: {leftover_chunk_data}")

            elif jsonobject['operation'] == 'read':
                filename = jsonobject['filename']
                offset = jsonobject['offset']
                num_bytes = jsonobject['num_bytes']
                print("poribus iste hic omnis magnam. Officia repudiandae unde neque voluptate explicabo, accusamus aspernatur, numquam mollitia nulla iure rerum voluptates! Ratione quia aliquam archi", offset, num_bytes)

                chunks_info = []
                start_chunk = offset // CHUNK_SIZE
                end_chunk = (offset + num_bytes - 1) // CHUNK_SIZE

                for chunk_index in range(start_chunk, end_chunk + 1):
                    if (filename, chunk_index) in mapping_for_finding_file:
                        chunk_mapping = mapping_for_finding_file[(filename, chunk_index)]
                        chunks_info.append({
                            "unique_id": chunk_mapping["unique_id"],
                            "servers": chunk_mapping["servers"],
                            "chunk_index": chunk_index
                        })
                    else:
                        print(f"Chunk {chunk_index} for file {filename} not found.")
                        chunks_info.append({"error": f"Chunk {chunk_index} not found."})

                response = {"chunks_info": chunks_info}
                response_data = pickle.dumps(response)
                client_socket.sendall(len(response_data).to_bytes(4, byteorder='big'))
                client_socket.sendall(response_data)

            elif jsonobject['operation'] == "append":

                filename = jsonobject['filename']
                data_to_append = jsonobject['data']
                # handle_append_transaction(client_socket, jsonobject['filename'], jsonobject['data'])
                # continue
                
                
                # Find the last chunk for this file
                last_chunk_index = -1
                for (file, chunk_index), chunk_mapping in mapping_for_finding_file.items():
                    if file == filename:
                        last_chunk_index = max(last_chunk_index, chunk_index)
                
                if last_chunk_index == -1:
                    response = {"error": "File not found"}
                else:
                    # Get the chunk servers for the last chunk
                    chunk_mapping = mapping_for_finding_file[(filename, last_chunk_index)]
                    
                    # Prepare response with chunk servers and last chunk index
                    response = {
                        "last_chunk_index": last_chunk_index,
                        "chunk_servers": chunk_mapping["servers"]
                    }
                
                # Serialize the response and send it back to the client
                response_data = pickle.dumps(response)
                client_socket.sendall(len(response_data).to_bytes(4, byteorder='big'))
                client_socket.sendall(response_data)



        except Exception as e:
            print(f"Error processing client request from {address}: {e}")
            break


# Function to handle chunk server connections
def handle_chunk_server(chunk_socket, address):
    available_chunks.add(address)
    print(f"Connected to chunk server: {address}")
    ping_thread = threading.Thread(target=ping_handler, args=(chunk_socket, address))
    ping_thread.start()


# Function to handle periodic pings from chunk servers
def ping_handler(chunk_socket, address):
    try:
        while True:
            length_data = chunk_socket.recv(4)
            if not length_data:
                print(f"Chunk server {address} disconnected.")
                available_chunks.remove(address)
                break
            
            length = int.from_bytes(length_data, byteorder='big')
            data = b''
            while len(data) < length:
                data += chunk_socket.recv(length - len(data))
            
            # Deserialize received data
            jsonobject = pickle.loads(data)
            chunk_server_port_for_client[address] = jsonobject['port']
            print(f"Updated port for chunk server {address}: {jsonobject['port']}")
            print(f"Available chunks from {address}: {jsonobject['chunks']}")
            
            time.sleep(10)  # Simulate periodic ping
    except Exception as e:
        print(f"Error receiving data from chunk server {address}: {e}")
        available_chunks.remove(address)


# Functions to listen for client and chunk server connections
def client_listener():
    client_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_server_socket.bind((CLIENT_HOST, CLIENT_PORT))
    client_server_socket.listen(5)
    print("Master server listening for clients...")
    while True:
        client_socket, address = client_server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(client_socket, address))
        thread.start()


def chunk_server_listener():
    chunk_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    chunk_server_socket.bind((CHUNK_HOST, CHUNK_PORT))
    chunk_server_socket.listen(5)
    print("Master server listening for chunk servers...")
    while True:
        chunk_socket, address = chunk_server_socket.accept()
        thread = threading.Thread(target=handle_chunk_server, args=(chunk_socket, address))
        thread.start()


if __name__ == "__main__":
    client_thread = threading.Thread(target=client_listener)
    chunk_thread = threading.Thread(target=chunk_server_listener)
    client_thread.start()
    chunk_thread.start()
    client_thread.join()
    chunk_thread.join()