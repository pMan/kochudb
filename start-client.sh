#1 /bin/bash

if [ -d "cli-client/target" ]; then
  java -jar cli-client/target/cli-client*.jar
else
  echo "Please build cli project first"
fi

