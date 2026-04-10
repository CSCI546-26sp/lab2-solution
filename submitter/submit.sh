#!/bin/bash

source ./env

FILE_PATH=$(./packer.sh $PROJ_ROOT | tail -1)

echo "Uploading $FILE_PATH" 
echo "Using secret: $SECRET"

curl -F "secret_key=$SECRET" -F "file=@$FILE_PATH" http://68.181.218.50:8000/upload
