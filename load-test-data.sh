#!/bin/bash

if [ -d "cli-client/target" ]; then
  java -jar cli-client/target/cli-client*.jar load
else
  echo "Please build client project first"
fi

