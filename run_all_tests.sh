#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Install pytest and requests-mock
pip install pytest requests-mock

# Run all tests
pytest tests/
