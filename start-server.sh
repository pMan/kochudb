#!/bin/bash

if [ -d "kochudb-server/target" ]; then
  java -Xmx2048m -jar kochudb-server/target/kochudb-server*.jar
else
  echo "Please build kochudb servert first"
fi

