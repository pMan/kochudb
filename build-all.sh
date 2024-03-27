#! /bin/bash

cd kochudb-commons && mvn clean package -DskipTests

cd ../kochudb-server && mvn clean package -DskipTests

cd ../cli-client && mvn clean package -DskipTests

