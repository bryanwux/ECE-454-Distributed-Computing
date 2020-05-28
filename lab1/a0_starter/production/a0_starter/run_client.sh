#!/bin/bash

export CLASSPATH=.:lib/*
./build.sh

echo --- Running
F=tiny
SAMPLE_INPUT=sample_input/$F.txt
SAMPLE_OUTPUT=sample_output/$F.out
SERVER_OUTPUT=myoutput.txt

echo -n "Enter the server's host name or IP address: "
read SERVER_HOST
echo -n "Enter the server's TCP port number: "
read SERVER_PORT

java CCClient $SERVER_HOST $SERVER_PORT $SAMPLE_INPUT $SERVER_OUTPUT

echo --- Comparing server\'s output against sample output
sort -o $SERVER_OUTPUT $SERVER_OUTPUT
diff $SERVER_OUTPUT $SAMPLE_OUTPUT
if [ $? -eq 0 ]; then
    echo Outputs match
else
    echo Outputs DO NOT match
fi
