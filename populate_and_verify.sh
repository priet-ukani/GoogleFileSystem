#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define client command
CLIENT_CMD="python client.py"

# --- Test 1: Create and Read Empty File ---
echo "\n--- Test 1: Create and Read Empty File ---"
echo "create"
echo "/test1.txt"
read -p "Press Enter after client creates /test1.txt..." # Pause for user input

echo "read"
echo "/test1.txt"
read -p "Press Enter after client reads /test1.txt (should be empty)..." # Pause for user input

# --- Test 2: Write and Read ---
echo "\n--- Test 2: Write and Read ---"
echo "write"
echo "/test2.txt"
echo "Hello World"
read -p "Press Enter after client writes 'Hello World' to /test2.txt..." # Pause for user input

echo "read"
echo "/test2.txt"
read -p "Press Enter after client reads /test2.txt (should be 'Hello World')..." # Pause for user input

# --- Test 3: Append ---
echo "\n--- Test 3: Append ---"
echo "append"
echo "/test2.txt"
echo " This is appended."
read -p "Press Enter after client appends ' This is appended.' to /test2.txt..." # Pause for user input

echo "read"
echo "/test2.txt"
read -p "Press Enter after client reads /test2.txt (should be 'Hello World This is appended.')..." # Pause for user input

# --- Test 4: List Files ---
echo "\n--- Test 4: List Files ---"
echo "ls"
echo "/"
read -p "Press Enter after client lists files (should show /test1.txt and /test2.txt)..." # Pause for user input

echo "\n--- All tests completed. ---"
