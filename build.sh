#!/bin/bash

check()
{
  if [ "$?" != "0" ]; then
    echo "**failed!" 1>&2
    exit 1
  fi
}
echo "building ..."
mvn clean install
check 
sbt publishLocal
check

