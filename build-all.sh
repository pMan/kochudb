#! /bin/bash

cd kochudb-server && mvn clean package -DskipTests

cd ../cli-client && mvn clean package -DskipTests

