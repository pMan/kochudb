#1 /bin/bash

if [ -d "kochudb-server/target" ]; then
  cd kochudb-server
  java -Xmx1024m -jar target/kochudb-server*.jar
else
  echo "Please build kochudb servert first"
fi

