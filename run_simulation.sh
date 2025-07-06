#!/bin/bash

# Define ports and data directories
MASTER_PORT=50052
CHUNK_SERVER_PORT_1=50053
CHUNK_SERVER_PORT_2=50054

CHUNK_SERVER_DATA_DIR_1="./chunk_data_1"
CHUNK_SERVER_DATA_DIR_2="./chunk_data_2"

MASTER_LOG="gfs_master.log"
CHUNK_SERVER_LOG_1="gfs_chunk_server_1.log"
CHUNK_SERVER_LOG_2="gfs_chunk_server_2.log"
CLIENT_LOG="gfs_client.log"

# Function to clean up processes and data
cleanup() {
    echo "\n--- Cleaning up GFS processes and data ---"
    kill $(jobs -p)
    rm -rf ${CHUNK_SERVER_DATA_DIR_1} ${CHUNK_SERVER_DATA_DIR_2}
    rm -f ${MASTER_LOG} ${CHUNK_SERVER_LOG_1} ${CHUNK_SERVER_LOG_2} ${CLIENT_LOG}
    rm -f gfs_metadata.db gfs_op.log
    echo "--- Cleanup complete ---"
}

# Trap Ctrl+C to call cleanup function
trap cleanup SIGINT

# Clean up from previous runs
cleanup

echo "--- Starting GFS Master Server ---"
python master_server.py > ${MASTER_LOG} 2>&1 &
MASTER_PID=$!
echo "Master server started with PID: ${MASTER_PID}"

echo "--- Starting GFS Chunk Server 1 ---"
python chunk_server.py ${CHUNK_SERVER_PORT_1} ${CHUNK_SERVER_DATA_DIR_1} > ${CHUNK_SERVER_LOG_1} 2>&1 &
CHUNK_SERVER_PID_1=$!
echo "Chunk server 1 started with PID: ${CHUNK_SERVER_PID_1}"

echo "--- Starting GFS Chunk Server 2 ---"
python chunk_server.py ${CHUNK_SERVER_PORT_2} ${CHUNK_SERVER_DATA_DIR_2} > ${CHUNK_SERVER_LOG_2} 2>&1 &
CHUNK_SERVER_PID_2=$!
echo "Chunk server 2 started with PID: ${CHUNK_SERVER_PID_2}"

echo "--- Giving servers time to initialize and register (10 seconds) ---"
sleep 10

echo "--- Running GFS Client Simulation ---"
python simulate_client.py > ${CLIENT_LOG} 2>&1

echo "--- GFS Client Simulation Finished ---"

# Keep the script running until Ctrl+C is pressed for manual inspection of logs
echo "--- All services are running. Press Ctrl+C to stop and clean up. ---"
wait
