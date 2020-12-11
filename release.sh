#!/bin/bash

rm -rf /tmp/mvnrepo

mvn versions:set -DnewVersion=$1 -DgenerateBackupPoms=false

git clone https://github.com/anthony-gascon/mvnrepo.git /tmp
mvn deploy -Durl=file:///tmp/mvnrepo -DcreateChecksum=true

cd /tmp/mvnrepo
git add -A . && git commit -m "released version $1"
git push

