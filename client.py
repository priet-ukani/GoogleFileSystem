import socket
import os
import pickle
MASTER_HOST = '127.0.0.1'
CLIENT_PORT = 5003  # Port for connecting to the master as a client
CHUNK_SIZE = 1024
EXACTLY_ONE_SEMATICS=0

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((MASTER_HOST, CLIENT_PORT))
    
    
    # upload file to master server
    
    # now here client takes infinitely takes input for file location
    while True:
        # give options to upload and read
        print("1. Upload file")
        print("2. Read file")
        print("3. Append")
        print("4. Exit")
        choice = int(input("Enter choice: "))
        if choice == 4:
            break
     
        elif choice == 2:
            file_name = input("Enter file name: ")
            file_size = os.path.getsize(file_location)

            # Create JSON object for read request
            jsonobject = {
                "operation": "read",
                "filename": file_name,
                "offset": 0,
                "num_bytes": file_size,
            }

            # Serialize and send the JSON object
            data = pickle.dumps(jsonobject)
            data_length = len(data)
            client_socket.sendall(data_length.to_bytes(4, byteorder='big'))
            client_socket.sendall(data)

            # Receive chunks information
            length_data = client_socket.recv(4)
            length = int.from_bytes(length_data, byteorder='big')
            data = b''
            while len(data) < length:
                chunk = client_socket.recv(length - len(data))
                if not chunk:
                    print("Error: Connection closed before receiving full response.")
                    client_socket.close()
                    exit()
                data += chunk

            chunks_info = pickle.loads(data)
            print(f"Received chunks information: {chunks_info}")

            # Retrieve file data chunk by chunk
            full_file_data = b''
            for chunk_info in chunks_info['chunks_info']:
                chunk_server = chunk_info['servers'][0]  # First server in the list
                
                # Create JSON object for chunk read request
                chunk_read_request = {
                    "operation": "read",
                    "chunk_index": chunk_info['chunk_index']
                }

                # Create a new socket to connect to the chunk server
                chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                chunk_socket.connect((MASTER_HOST, chunk_server))

                # Send chunk read request
                chunk_request_data = pickle.dumps(chunk_read_request)
                chunk_request_length = len(chunk_request_data)
                chunk_socket.sendall(chunk_request_length.to_bytes(4, byteorder='big'))
                chunk_socket.sendall(chunk_request_data)

                # Receive chunk data
                chunk_length_data = chunk_socket.recv(4)
                chunk_length = int.from_bytes(chunk_length_data, byteorder='big')
                chunk_data = b''
                while len(chunk_data) < chunk_length:
                    chunk = chunk_socket.recv(chunk_length - len(chunk_data))
                    if not chunk:
                        print(f"Error: Connection closed before receiving chunk {chunk_info['chunk_index']}.")
                        chunk_socket.close()
                        exit()
                    chunk_data += chunk

                chunk_response = pickle.loads(chunk_data)
                print(f"Received chunk {chunk_info['chunk_index']} data")
                
                # Append chunk data to full file data
                full_file_data += chunk_response['data']
                
                chunk_socket.close()

            # Remove padding if any
            full_file_data = full_file_data.rstrip(b'8')
            
            print("Full file data:")
            print(full_file_data.decode())

                    
        elif choice == 1:
            file_location = input("Enter file location: ")

            # Get the file size and calculate the number of chunks
            file_size = os.path.getsize(file_location)
            num_chunks = file_size // CHUNK_SIZE
            if file_size % CHUNK_SIZE != 0:
                num_chunks += 1
                last_chunk_size = file_size % CHUNK_SIZE

            jsonobject = {
                "operation": "upload",
                "filename": file_location,
                "num_chunks": num_chunks,
                "last_chunk_size": last_chunk_size,
            }

            # Serialize and send the JSON object
            data = pickle.dumps(jsonobject)
            data_length = len(data)

            # Send the length of the data (as 4 bytes)
            client_socket.sendall(data_length.to_bytes(4, byteorder='big'))  # Send length as 4 bytes
            client_socket.sendall(data)  # Send the actual serialized data

            # Receive the mapping from the server
            length_data = client_socket.recv(4)  # Receive 4 bytes for the length
            length = int.from_bytes(length_data, byteorder='big')  # Convert 4 bytes to integer

            # Receive the actual response data based on the length
            data = b''
            while len(data) < length:
                data += client_socket.recv(length - len(data))

            # Deserialize the received data (mappings)
            mappings = pickle.loads(data)
            print(f"Received mappings: {mappings}")
            
            
            # after client gets the mappings now client has to send this chunk to respective chunk servers
            # start reading the file chunk wise and start sending json object to chunk server
            
            
            with open(file_location, 'rb') as file:
                for i in range(num_chunks):
                    chunk_data = file.read(CHUNK_SIZE)
                    chunk_jsonobject = {
                        "operation": "upload",
                        "chunk_number": mappings['mapping'][i][0],
                        "chunk_servers": mappings['mapping'][i][1:],
                        "data": chunk_data
                    }
                    print(f"Sending chunk {i} to chunk servers {mappings['mapping'][i][1:]}")
                    
                    # send this to chunk server in the mapping array
                    # create connections
                    
                    client_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client_socket2.connect((MASTER_HOST, mappings['mapping'][i][1]))
                    chunk_data = pickle.dumps(chunk_jsonobject)
                    chunk_data_length = len(chunk_data)
                    client_socket2.sendall(chunk_data_length.to_bytes(4, byteorder='big'))
                    client_socket2.sendall(chunk_data)
        
        elif choice == 3:
            file_name = input("Enter file name: ")
            data_append = input("Enter data to append: ")
            
            # Send append request to master server
            jsonobject = {
                "operation": "append",
                "filename": file_name,
                "data": data_append
            }
            
            # Serialize and send the JSON object
            data = pickle.dumps(jsonobject)
            data_length = len(data)
            client_socket.sendall(data_length.to_bytes(4, byteorder='big'))
            client_socket.sendall(data)
            
            # Receive chunk server information for appending
            length_data = client_socket.recv(4)
            length = int.from_bytes(length_data, byteorder='big')
            data = b''
            while len(data) < length:
                chunk = client_socket.recv(length - len(data))
                if not chunk:
                    print("Error: Connection closed before receiving full response.")
                    client_socket.close()
                    exit()
                data += chunk
            
            append_info = pickle.loads(data)
            
            if 'error' in append_info:
                print(f"Error: {append_info['error']}")
            else:
                last_chunk_index = append_info['last_chunk_index']
                chunk_servers = append_info['chunk_servers']
                
                for i in range(2):
                    # Send append request to the first chunk server
                    chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    chunk_socket.connect((MASTER_HOST, chunk_servers[i]))
                    
                    append_jsonobject = {
                        "operation": "append",
                        "chunk_index": last_chunk_index,
                        "data": data_append
                    }
                    
                    # Serialize and send append request
                    chunk_request_data = pickle.dumps(append_jsonobject)
                    chunk_request_length = len(chunk_request_data)
                    chunk_socket.sendall(chunk_request_length.to_bytes(4, byteorder='big'))
                    chunk_socket.sendall(chunk_request_data)
                    
                    # Receive response from chunk server
                    chunk_length_data = chunk_socket.recv(4)
                    chunk_length = int.from_bytes(chunk_length_data, byteorder='big')
                    chunk_data = b''
                    while len(chunk_data) < chunk_length:
                        chunk = chunk_socket.recv(chunk_length - len(chunk_data))
                        if not chunk:
                            print("Error: Connection closed before receiving response.")
                            chunk_socket.close()
                            exit()
                        chunk_data += chunk
                    
                    chunk_response = pickle.loads(chunk_data)
                    print(f"Append response: {chunk_response['message']}")
                    
                    chunk_socket.close()


        elif choice == 3 and EXACTLY_ONE_SEMATICS: 
            file_name = input("Enter file name: ")
            data_append = input("Enter data to append: ")
            
            # Send append request to master server
            jsonobject = {
                "operation": "append",
                "filename": file_name,
                "data": data_append
            }
            
            # Serialize and send the JSON object
            data = pickle.dumps(jsonobject)
            data_length = len(data)
            client_socket.sendall(data_length.to_bytes(4, byteorder='big'))
            client_socket.sendall(data)
            
            # Receive transaction information from master server
            length_data = client_socket.recv(4)
            length = int.from_bytes(length_data, byteorder='big')
            data = b''
            while len(data) < length:
                chunk = client_socket.recv(length - len(data))
                if not chunk:
                    print("Error: Connection closed before receiving full response.")
                    client_socket.close()
                    exit()
                data += chunk
            
            append_info = pickle.loads(data)
            
            if 'error' in append_info:
                print(f"Error: {append_info['error']}")
            else:
                transaction_id = append_info['transaction_id']
                last_chunk_index = append_info['last_chunk_index']
                chunk_servers = append_info['chunk_servers']
                
                # Prepare phase
                prepare_responses = []
                for chunk_server in chunk_servers:
                    chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    chunk_socket.connect((MASTER_HOST, chunk_server))
                    
                    prepare_request = {
                        "operation": "prepare_append",
                        "transaction_id": transaction_id,
                        "chunk_index": last_chunk_index,
                        "data": data_append
                    }
                    
                    chunk_request_data = pickle.dumps(prepare_request)
                    chunk_request_length = len(chunk_request_data)
                    chunk_socket.sendall(chunk_request_length.to_bytes(4, byteorder='big'))
                    chunk_socket.sendall(chunk_request_data)
                    
                    # Receive prepare response
                    chunk_length_data = chunk_socket.recv(4)
                    chunk_length = int.from_bytes(chunk_length_data, byteorder='big')
                    chunk_data = b''
                    while len(chunk_data) < chunk_length:
                        chunk = chunk_socket.recv(chunk_length - len(chunk_data))
                        if not chunk:
                            print("Error: Connection closed before receiving response.")
                            chunk_socket.close()
                            exit()
                        chunk_data += chunk
                    
                    prepare_response = pickle.loads(chunk_data)
                    prepare_responses.append(prepare_response)
                    chunk_socket.close()
                
                # Check if all prepare responses are READY
                if all(response['status'] == 'READY' for response in prepare_responses):
                    # Commit phase
                    for chunk_server in chunk_servers:
                        chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        chunk_socket.connect((MASTER_HOST, chunk_server))
                        
                        commit_request = {
                            "operation": "commit_append",
                            "transaction_id": transaction_id,
                            "chunk_index": last_chunk_index,
                            "data": data_append
                        }
                        
                        commit_request_data = pickle.dumps(commit_request)
                        commit_request_length = len(commit_request_data)
                        chunk_socket.sendall(commit_request_length.to_bytes(4, byteorder='big'))
                        chunk_socket.sendall(commit_request_data)
                        
                        # Receive commit response
                        commit_length_data = chunk_socket.recv(4)
                        commit_length = int.from_bytes(commit_length_data, byteorder='big')
                        commit_data = b''
                        while len(commit_data) < commit_length:
                            chunk = chunk_socket.recv(commit_length - len(commit_data))
                            if not chunk:
                                print("Error: Connection closed before receiving response.")
                                chunk_socket.close()
                                exit()
                            commit_data += chunk
                        
                        commit_response = pickle.loads(commit_data)
                        print(f"Append commit response: {commit_response}")
                        chunk_socket.close()
                else:
                    # Abort phase if any prepare response is not READY
                    for chunk_server in chunk_servers:
                        chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        chunk_socket.connect((MASTER_HOST, chunk_server))
                        
                        abort_request = {
                            "operation": "abort_append",
                            "transaction_id": transaction_id
                        }
                        
                        abort_request_data = pickle.dumps(abort_request)
                        abort_request_length = len(abort_request_data)
                        chunk_socket.sendall(abort_request_length.to_bytes(4, byteorder='big'))
                        chunk_socket.sendall(abort_request_data)
                        
                        # Receive abort response
                        abort_length_data = chunk_socket.recv(4)
                        abort_length = int.from_bytes(abort_length_data, byteorder='big')
                        abort_data = b''
                        while len(abort_data) < abort_length:
                            chunk = chunk_socket.recv(abort_length - len(abort_data))
                            if not chunk:
                                print("Error: Connection closed before receiving response.")
                                chunk_socket.close()
                                exit()
                            abort_data += chunk
                        
                        abort_response = pickle.loads(abort_data)
                        print(f"Append transaction aborted: {abort_response}")
                        chunk_socket.close()

    

if __name__ == "__main__":
    start_client()