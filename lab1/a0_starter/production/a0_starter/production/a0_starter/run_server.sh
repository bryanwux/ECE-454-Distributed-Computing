#!/bin/bash

export CLASSPATH=.:lib/*
./build.sh

echo --- Running
RANDOM_PORT=`shuf -i 10000-10999 -n 1`
echo randomly chose port $RANDOM_PORT
java -Xmx1g CCServer $RANDOM_PORT
