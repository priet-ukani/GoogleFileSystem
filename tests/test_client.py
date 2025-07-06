import pytest
import requests_mock
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from client import GFSClient
import config

@pytest.fixture
def client():
    return GFSClient()

@pytest.fixture
def master_url():
    return f"http://{config.MASTER_HOST}:{config.MASTER_PORT}"

def test_create_success(client, master_url):
    with requests_mock.Mocker() as m:
        m.post(f"{master_url}/create", status_code=200)
        assert client.create("/testfile.txt") is True

def test_create_failure(client, master_url):
    with requests_mock.Mocker() as m:
        m.post(f"{master_url}/create", status_code=500)
        assert client.create("/testfile.txt") is False

def test_write_success(client, master_url):
    with requests_mock.Mocker() as m:
        # Mock get_chunk_locations
        m.get(f"{master_url}/get_chunk_locations", json={
            'chunk_handle': '123',
            'locations': [50001, 50002],
            'primary': 50001
        }, status_code=200)
        # Mock chunk server write
        m.post("http://127.0.0.1:50001/write", status_code=200)
        m.post("http://127.0.0.1:50002/write", status_code=200)
        assert client.write("/testfile.txt", "hello") is True

def test_read_success(client, master_url):
    with requests_mock.Mocker() as m:
        # Mock get_chunk_locations
        m.get(f"{master_url}/get_chunk_locations", json={
            'chunk_handle': '123',
            'locations': [50001, 50002],
            'primary': 50001
        }, status_code=200)
        # Mock chunk server read
        m.get("http://127.0.0.1:50001/read", json={'data': 'test data'}, status_code=200)
        content = client.read("/testfile.txt")
        assert content == 'test data'

def test_append_success(client, master_url):
    with requests_mock.Mocker() as m:
        # Mock get_file_info
        m.get(f"{master_url}/get_file_info", json={'length': 5}, status_code=200)
        # Mock get_chunk_locations
        m.get(f"{master_url}/get_chunk_locations", json={
            'chunk_handle': '123',
            'locations': [50001, 50002],
            'primary': 50001
        }, status_code=200)
        # Mock chunk server write
        m.post("http://127.0.0.1:50001/write", status_code=200)
        m.post("http://127.0.0.1:50002/write", status_code=200)
        # Mock update_file_length
        m.post(f"{master_url}/update_file_length", status_code=200)
        assert client.append("/testfile.txt", " world") is True

def test_ls_success(client, master_url):
    with requests_mock.Mocker() as m:
        m.get(f"{master_url}/ls", json=["/file1.txt", "/file2.txt"], status_code=200)
        files = client.ls("/")
        assert files == ["/file1.txt", "/file2.txt"]
