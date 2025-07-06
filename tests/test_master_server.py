import pytest
import sys
import os
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from master_server import GFSMaster
import config

@pytest.fixture
def master():
    # Clean up metadata before each test
    if os.path.exists(config.METADATA_STORE):
        os.remove(config.METADATA_STORE)
    if os.path.exists(config.OPERATION_LOG):
        os.remove(config.OPERATION_LOG)
    return GFSMaster()

def test_create_file(master):
    assert master.create_file("/testfile.txt") is not None
    assert "/testfile.txt" in master.files

def test_create_existing_file(master):
    master.create_file("/testfile.txt")
    assert master.create_file("/testfile.txt") is None

def test_register_chunk_server(master):
    server_id = master.register_chunk_server(50001, "/data/chunk1")
    assert server_id in master.chunk_servers
    assert master.chunk_servers[server_id]['port'] == 50001

def test_allocate_chunk(master):
    master.register_chunk_server(50001, "/data/chunk1")
    master.register_chunk_server(50002, "/data/chunk2")
    master.create_file("/testfile.txt")
    chunk_info = master.allocate_chunk("/testfile.txt", 0)
    assert chunk_info is not None
    assert 'chunk_handle' in chunk_info
    assert 'locations' in chunk_info
    assert 'primary' in chunk_info

def test_get_chunk_locations(master):
    master.register_chunk_server(50001, "/data/chunk1")
    master.register_chunk_server(50002, "/data/chunk2")
    master.create_file("/testfile.txt")
    master.allocate_chunk("/testfile.txt", 0)
    locations = master.get_chunk_locations("/testfile.txt", 0)
    assert locations is not None
    assert 'chunk_handle' in locations
    assert 'locations' in locations
    assert 'primary' in locations

def test_get_file_info(master):
    master.create_file("/testfile.txt")
    master.files["/testfile.txt"]['length'] = 100 # Manually set length for testing
    info = master.get_file_info("/testfile.txt")
    assert info == {'length': 100}

def test_update_file_length(master):
    master.create_file("/testfile.txt")
    master.update_file_length("/testfile.txt", 200)
    assert master.files["/testfile.txt"]['length'] == 200
