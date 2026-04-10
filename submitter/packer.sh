#!/bin/bash

ROOT_DIR=$1

cd $ROOT_DIR
EXCLUDE="CMakeLists.txt"
tar --exclude=$EXCLUDE -czvf pack.tar.gz inc/rafty proto/ src/
echo "$PWD/pack.tar.gz"