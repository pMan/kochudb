#! /bin/bash

cd kochudb-commons && mvn clean install -DskipTests

cd ../kochudb-server && mvn clean install -DskipTests

cd ../cli-client && mvn clean install -DskipTests

