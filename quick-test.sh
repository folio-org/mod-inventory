#!/usr/bin/env bash

rm -rf target/

mvn -q clean org.jacoco:jacoco-maven-plugin:prepare-agent test

test_results=$?

if [ $test_results != 0 ]; then
  echo '--------------------------------------'
  echo 'BUILD FAILED'
  echo '--------------------------------------'
  exit 1;
else
  echo '--------------------------------------'
  echo 'BUILD SUCCEEDED'
  echo '--------------------------------------'
fi
