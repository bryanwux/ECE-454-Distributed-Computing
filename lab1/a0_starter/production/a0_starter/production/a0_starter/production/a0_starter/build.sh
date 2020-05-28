#!/bin/bash

export CLASSPATH=.:lib/*

echo --- Deleting
rm -f *.jar
rm -f *.class

echo --- Compiling
javac *.java
if [ $? -ne 0 ]; then
    exit
fi
