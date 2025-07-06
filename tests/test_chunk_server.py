import pytest
import sys
import os
import shutil
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from chunk_server import GFSChunkServer

@pytest.fixture
def chunk_server_instance():
    test_data_dir = "./test_chunk_data"
    if os.path.exists(test_data_dir):
        shutil.rmtree(test_data_dir)
    os.makedirs(test_data_dir)
    server = GFSChunkServer(port=50001, data_dir=test_data_dir)
    yield server
    shutil.rmtree(test_data_dir)

def test_write_and_read_chunk(chunk_server_instance):
    chunk_handle = "test_handle_1"
    data_to_write = "Hello, GFS!"
    
    # Simulate write operation
    chunk_server_instance._handle_write({'chunk_handle': chunk_handle, 'data': data_to_write, 'offset': 0})
    
    # Read and verify
    read_data = chunk_server_instance.read_chunk(chunk_handle)
    assert read_data == data_to_write

def test_append_chunk(chunk_server_instance):
    chunk_handle = "test_handle_2"
    initial_data = "First part."
    append_data = " Second part."
    
    # Simulate initial write
    chunk_server_instance._handle_write({'chunk_handle': chunk_handle, 'data': initial_data, 'offset': 0})
    
    # Simulate append operation
    chunk_server_instance._handle_append({'request_id': 'req1', 'chunk_handle': chunk_handle, 'data': append_data})
    
    # Read and verify
    read_data = chunk_server_instance.read_chunk(chunk_handle)
    assert read_data == initial_data + append_data

def test_load_and_save_metadata(chunk_server_instance):
    chunk_handle = "test_handle_3"
    data_to_write = "Metadata test."
    
    chunk_server_instance._handle_write({'chunk_handle': chunk_handle, 'data': data_to_write, 'offset': 0})
    chunk_server_instance.save_metadata()
    
    # Create a new instance to load metadata
    new_server = GFSChunkServer(port=50002, data_dir=chunk_server_instance.data_dir)
    new_server.load_metadata()
    
    assert chunk_handle in new_server.chunks
    assert new_server.chunks[chunk_handle]['version'] == 1
