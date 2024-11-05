#!/bin/bash

if [ -d "cli-client/target" ]; then
  java -jar cli-client/target/cli-client*.jar
else
  echo "Please build client project first"
fi

