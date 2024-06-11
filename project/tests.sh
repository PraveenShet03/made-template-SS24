#!/bin/bash

# Fail the script on the first error
set -e

# Install the required dependencies
echo "Installing dependencies..."
pip install -r project/requirements.txt

# Run the tests
echo "Running tests..."
pytest project/test_etl_pipeline.py

echo "All tests passed!"
