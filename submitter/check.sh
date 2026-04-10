#!/bin/bash

source ./env

echo "Using secret: $SECRET"

curl -F "secret_key=$SECRET" http://68.181.218.50:8000/check
