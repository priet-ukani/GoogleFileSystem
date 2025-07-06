import time
import random
from client import GFSClient

def run_simulation():
    client = GFSClient()

    print("--- Starting GFS Client Simulation ---")

    # 1. Create a file
    filename1 = "test_file_1.txt"
    print(f"Creating file: {filename1}")
    if client.create(filename1):
        print(f"Successfully created {filename1}")
    else:
        print(f"Failed to create {filename1}")
        return

    # 2. Write some data to the file
    data1 = "Hello GFS! This is the first chunk of data."
    print(f"Writing data to {filename1}: '{data1}'")
    if client.write(filename1, data1):
        print(f"Successfully wrote to {filename1}")
    else:
        print(f"Failed to write to {filename1}")
        return

    # 3. Append more data
    data2 = " And this is appended data for the second part."
    print(f"Appending data to {filename1}: '{data2}'")
    if client.append(filename1, data2):
        print(f"Successfully appended to {filename1}")
    else:
        print(f"Failed to append to {filename1}")
        return

    # 4. Read the entire file
    print(f"Reading content of {filename1}")
    content = client.read(filename1)
    if content is not None:
        print(f"Content of {filename1}: '{content}'")
        expected_content = data1 + data2
        if content == expected_content:
            print("Read content matches expected content.")
        else:
            print(f"Read content MISMATCH! Expected: '{expected_content}', Got: '{content}'")
    else:
        print(f"Failed to read {filename1}")
        return

    # 5. Create another file and write a lot of data to test multiple chunks
    filename2 = "large_test_file.txt"
    print(f"Creating file: {filename2}")
    if client.create(filename2):
        print(f"Successfully created {filename2}")
    else:
        print(f"Failed to create {filename2}")
        return

    # Write data that spans multiple chunks (assuming CHUNK_SIZE_BYTES is 64KB)
    # Let's write 3 chunks worth of data
    large_data = "A" * (64 * 1024) + "B" * (64 * 1024) + "C" * (64 * 1024)
    print(f"Writing large data ({len(large_data)} bytes) to {filename2}")
    if client.write(filename2, large_data):
        print(f"Successfully wrote large data to {filename2}")
    else:
        print(f"Failed to write large data to {filename2}")
        return

    # 6. Read a portion of the large file
    print(f"Reading first 64KB of {filename2}")
    partial_content = client.read(filename2, offset=0, length=64*1024)
    if partial_content is not None:
        print(f"First 64KB of {filename2}: '{partial_content[:50]}...' (truncated for display)")
        if partial_content == "A" * (64 * 1024):
            print("Partial read content matches expected.")
        else:
            print("Partial read content MISMATCH!")
    else:
        print(f"Failed to read partial content from {filename2}")

    # 7. List files
    print("Listing all files:")
    files = client.ls("/")
    if files is not None:
        for f in files:
            print(f"  - {f}")
    else:
        print("Failed to list files.")

    print("--- GFS Client Simulation Complete ---")

if __name__ == "__main__":
    # Give some time for servers to start up
    time.sleep(5)
    run_simulation()
