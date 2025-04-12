import socket
import threading
import time
import pickle
import random

# Existing configurations remain the same
MASTER_HOST = '127.0.0.1'
MASTER_PORT = 5004
CHUNK_SIZE = 1024
chunks = []
random_port = random.randint(10000, 20000)

class ExactlyOnceAppendProtocol:
    def __init__(self):
        self.active_transactions = {}
        self.completed_transactions = {}
        self.transaction_timeout = 60  # 1-minute timeout

    def validate_transaction(self, transaction_id):
        # Check if transaction has already been committed
        if transaction_id in self.completed_transactions:
            return "ALREADY_COMMITTED"

        # Check for timeout
        transaction = self.active_transactions.get(transaction_id)
        if transaction and time.time() - transaction.get('timestamp', 0) > self.transaction_timeout:
            return "TIMEOUT"

        return "VALID"

    def record_transaction(self, transaction_id, chunk_index, data):
        self.active_transactions[transaction_id] = {
            "chunk_index": chunk_index,
            "data": data,
            "timestamp": time.time(),
            "status": "PREPARED"
        }

    def commit_transaction(self, transaction_id):
        if transaction_id in self.active_transactions:
            transaction = self.active_transactions[transaction_id]
            self.completed_transactions[transaction_id] = transaction
            del self.active_transactions[transaction_id]

    def abort_transaction(self, transaction_id):
        if transaction_id in self.active_transactions:
            del self.active_transactions[transaction_id]

# Initialize exactly-once append protocol
exactly_once_append_protocol = ExactlyOnceAppendProtocol()

def handle_prepare_append(client_socket, jsonobject):
    transaction_id = jsonobject.get("transaction_id")
    chunk_index = jsonobject.get("chunk_index")
    data_to_append = jsonobject.get("data")
    
    transaction_status = exactly_once_append_protocol.validate_transaction(transaction_id)
    
    if transaction_status == "ALREADY_COMMITTED":
        response = {
            "status": "COMMITTED",
            "transaction_id": transaction_id,
            "message": "Transaction already committed"
        }
    elif transaction_status == "TIMEOUT":
        response = {
            "status": "ABORT",
            "transaction_id": transaction_id,
            "reason": "Transaction timeout"
        }
    else:
        try:
            # Verify chunk exists
            with open(f"chunk_server_{random_port}", 'rb') as file:
                chunks_data = file.readlines()
            
            if 0 <= chunk_index < len(chunks_data):
                # Record transaction details
                exactly_once_append_protocol.record_transaction(transaction_id, chunk_index, data_to_append)
                
                response = {
                    "status": "READY",
                    "transaction_id": transaction_id
                }
            else:
                response = {
                    "status": "ABORT",
                    "transaction_id": transaction_id,
                    "reason": "Invalid chunk index"
                }
        
        except Exception as e:
            response = {
                "status": "ABORT",
                "transaction_id": transaction_id,
                "reason": str(e)
            }
    
    # Serialize and send the response
    chunk_response = pickle.dumps(response)
    chunk_response_length = len(chunk_response)
    client_socket.sendall(chunk_response_length.to_bytes(4, byteorder='big'))
    client_socket.sendall(chunk_response)

def handle_commit_append(client_socket, jsonobject):
    transaction_id = jsonobject.get("transaction_id")
    chunk_index = jsonobject.get("chunk_index")
    
    try:
        # Validate transaction status before committing
        transaction_status = exactly_once_append_protocol.validate_transaction(transaction_id)
        
        if transaction_status != "VALID":
            raise ValueError(f"Invalid transaction status: {transaction_status}")
        
        # Retrieve transaction details
        transaction = exactly_once_append_protocol.active_transactions.get(transaction_id)
        if not transaction:
            raise ValueError("Transaction not found")
        
        data_to_append = transaction['data']
        
        # Read the specific chunk from the chunk server's file
        with open(f"chunk_server_{random_port}", 'rb') as file:
            chunks_data = file.readlines()
        
        if 0 <= chunk_index < len(chunks_data):
            # Remove padding from the last chunk
            last_chunk = chunks_data[chunk_index].rstrip(b'\n').rstrip(b'8')
            
            # Combine existing chunk data with new data
            new_chunk_data = last_chunk + data_to_append.encode()
            
            # Pad the new chunk to CHUNK_SIZE
            if len(new_chunk_data) < CHUNK_SIZE:
                new_chunk_data = new_chunk_data.ljust(CHUNK_SIZE, b'8')
            
            # Update the chunk in the file
            with open(f"chunk_server_{random_port}", 'r+b') as file:
                file.seek(chunk_index * (CHUNK_SIZE + 1))  # +1 for newline
                file.write(new_chunk_data + b'\n')
            
            # Mark transaction as committed
            exactly_once_append_protocol.commit_transaction(transaction_id)
            
            response = {
                "status": "COMMITTED",
                "transaction_id": transaction_id,
                "message": "Data appended successfully"
            }
        else:
            response = {
                "status": "ABORT",
                "transaction_id": transaction_id,
                "reason": "Invalid chunk index"
            }
    
    except Exception as e:
        response = {
            "status": "ABORT",
            "transaction_id": transaction_id,
            "reason": str(e)
        }
    
    # Serialize and send the response
    chunk_response = pickle.dumps(response)
    chunk_response_length = len(chunk_response)
    client_socket.sendall(chunk_response_length.to_bytes(4, byteorder='big'))
    client_socket.sendall(chunk_response)

def handle_abort_append(client_socket, jsonobject):
    transaction_id = jsonobject.get("transaction_id")
    
    try:
        # Abort the transaction
        exactly_once_append_protocol.abort_transaction(transaction_id)
        
        response = {
            "status": "ABORTED",
            "transaction_id": transaction_id
        }
    except Exception as e:
        response = {
            "status": "ERROR",
            "transaction_id": transaction_id,
            "reason": str(e)
        }
    
    # Serialize and send the response
    chunk_response = pickle.dumps(response)
    chunk_response_length = len(chunk_response)
    client_socket.sendall(chunk_response_length.to_bytes(4, byteorder='big'))
    client_socket.sendall(chunk_response)

# Modify handle_client to use new append transaction methods
def handle_client(client_socket, address):
    while True:
        try:
            length_data = client_socket.recv(4)
            if len(length_data) < 4:
                print(f"Incomplete length data received from {address}")
                break
            
            length = int.from_bytes(length_data, byteorder='big')
            data = b''
            while len(data) < length:
                data += client_socket.recv(length - len(data))
            
            jsonobject = pickle.loads(data)
            print(f"Received request from {address}: {jsonobject}")
            
            # Handle append transaction operations
            if jsonobject["operation"] == "prepare_append":
                handle_prepare_append(client_socket, jsonobject)
            
            elif jsonobject["operation"] == "commit_append":
                handle_commit_append(client_socket, jsonobject)
            
            elif jsonobject["operation"] == "abort_append":
                handle_abort_append(client_socket, jsonobject)
                
                
            # Deserialize the data using pickle
            try:
                jsonobject = pickle.loads(data)
                print(f"Received request from {address}: {jsonobject}")
                
                if(jsonobject["operation"] == "upload"):
                
                    chunk_data = jsonobject["data"]
                    if not isinstance(chunk_data, bytes):
                        chunk_data = chunk_data.encode()  # Convert to bytes if it's a string

                    # Pad the chunk data to CHUNK_SIZE with b'8'
                    if len(chunk_data) < CHUNK_SIZE:
                        chunk_data = chunk_data.ljust(CHUNK_SIZE, b'8')  # Pad with b'8'

                    # Write the chunk to a file named "chunk_server_{port}"
                    with open(f"chunk_server_{random_port}", 'ab') as file:
                        file.write(chunk_data + b'\n')  # Write bytes with newline

                    chunks.append(jsonobject['chunk_number'])
                    # remove the first entry from the json object array
                    jsonobject['chunk_servers'].pop(0)
                
                    # send this to the next chunk server in the array
                    if len(jsonobject['chunk_servers']) > 0:
                        next_chunk_server = jsonobject['chunk_servers'][0]
                        client_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client_socket2.connect((MASTER_HOST, next_chunk_server))
                        chunk_data = pickle.dumps(jsonobject)
                        chunk_data_length = len(chunk_data)
                        client_socket2.sendall(chunk_data_length.to_bytes(4, byteorder='big'))
                        client_socket2.sendall(chunk_data)

                elif jsonobject["operation"] == "read":
                    chunk_index = jsonobject.get("chunk_index")
                    print(f"Reading chunk {chunk_index}")
                    
                    # Read the specific chunk from the chunk server's file
                    with open(f"chunk_server_{random_port}", 'rb') as file:
                        chunks_data = file.readlines()
                        if 0 <= chunk_index < len(chunks_data):
                            chunk_data = chunks_data[chunk_index].rstrip(b'\n')
                            
                            # Create response object
                            response = {
                                "data": chunk_data
                            }
                            
                            # Serialize and send the chunk data
                            chunk_response = pickle.dumps(response)
                            chunk_response_length = len(chunk_response)
                            client_socket.sendall(chunk_response_length.to_bytes(4, byteorder='big'))
                            client_socket.sendall(chunk_response)
                        else:
                            print(f"Chunk {chunk_index} not found")

    # ok ssahil
                elif jsonobject["operation"] == 'append':
                    chunk_index = jsonobject.get("chunk_index")
                    data_to_append = jsonobject.get("data")
                    
                    try:
                        # Read the specific chunk from the chunk server's file
                        with open(f"chunk_server_{random_port}", 'rb') as file:
                            chunks_data = file.readlines()
                        
                        if 0 <= chunk_index < len(chunks_data):
                            # Remove padding from the last chunk
                            last_chunk = chunks_data[chunk_index].rstrip(b'\n').rstrip(b'8')
                            
                            # Combine existing chunk data with new data
                            new_chunk_data = last_chunk + data_to_append.encode()
                            
                            # Pad the new chunk to CHUNK_SIZE
                            if len(new_chunk_data) < CHUNK_SIZE:
                                new_chunk_data = new_chunk_data.ljust(CHUNK_SIZE, b'8')
                            
                            # Update the chunk in the file
                            with open(f"chunk_server_{random_port}", 'r+b') as file:
                                # Seek to the start of the specific chunk
                                file.seek(chunk_index * (CHUNK_SIZE + 1))  # +1 for newline
                                file.write(new_chunk_data + b'\n')
                                print("Appended to file. ", new_chunk_data)
                            
                            # Prepare and send the response
                            response = {
                                "status": "success",
                                "message": "Data appended successfully"
                            }
                        else:
                            response = {
                                "status": "error",
                                "message": f"Chunk {chunk_index} not found"
                            }
                        
                        # Serialize and send the response
                        chunk_response = pickle.dumps(response)
                        chunk_response_length = len(chunk_response)
                        client_socket.sendall(chunk_response_length.to_bytes(4, byteorder='big'))
                        client_socket.sendall(chunk_response)
                    
                    except Exception as e:
                        print(f"Error during append operation: {e}")
                        response = {
                            "status": "error",
                            "message": str(e)
                        }
                        chunk_response = pickle.dumps(response)
                        chunk_response_length = len(chunk_response)
                        client_socket.sendall(chunk_response_length.to_bytes(4, byteorder='big'))
                        client_socket.sendall(chunk_response)
                
            except Exception as e:
                print(f"Error processing client data: {e}")
        except:
            print("error")
# Function to send the chunks array and port to the master server
def ping_master(master_socket):
    while True:
        try:   
            jsonobject = {"chunks": chunks, "port": random_port}
            data = pickle.dumps(jsonobject)
            length = len(data).to_bytes(4, byteorder='big')  # 4-byte length header
            master_socket.sendall(length)
            master_socket.sendall(data)
            print("Sent chunks to master server.")
        except Exception as e:
            print(f"Error sending data to master: {e}")
            break
        time.sleep(10)

# Function to handle connection with the master server
def handle_master_connection():
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_socket.connect((MASTER_HOST, MASTER_PORT))
    print("Chunk server connected to master.")

    # Start pinging the master in a separate thread
    ping_thread = threading.Thread(target=ping_master, args=(master_socket,))
    ping_thread.start()
    
    try:
        while True:
            pass  # Placeholder for additional master communication
    except Exception as e:
        print(f"Error with master server connection: {e}")
    finally:
        master_socket.close()
        print("Disconnected from master server.")

# Function to listen for client connections on random_port
def client_listener():
    client_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_server_socket.bind(('0.0.0.0', random_port))
    client_server_socket.listen(5)
    print(f"Chunk server listening for clients on port {random_port}...")

    while True:
        client_socket, address = client_server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_socket, address))
        client_thread.start()

if __name__ == "__main__":
    # Start a thread to handle connection with the master server
    master_thread = threading.Thread(target=handle_master_connection)
    master_thread.start()

    # Start the client listener on the randomly generated port
    client_listener_thread = threading.Thread(target=client_listener)
    client_listener_thread.start()

    master_thread.join()
    client_listener_thread.join()