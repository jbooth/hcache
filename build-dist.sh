#!/bin/sh

# simple shell script to create a distribution in dist
mvn package

rm -rf dist
mkdir dist
cp target/*.jar dist/.
cp bin/* dist/.
cp INSTALLATION.txt dist/.
