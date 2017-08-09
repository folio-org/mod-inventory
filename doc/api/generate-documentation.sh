#!/usr/bin/env bash

git submodule init
git submodule update

npm install

rm -rf generated

mkdir generated
mkdir generated/html
mkdir generated/md

node_modules/raml2html/bin/raml2html -i ../../ramls/inventory.raml -o generated/html/inventory.html

node_modules/raml2md/bin/raml2md -i ../../ramls/inventory.raml -o generated/md/inventory.md
