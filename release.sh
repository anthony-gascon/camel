#!/bin/bash



mvn versions:set -DnewVersion=$1 -DgenerateBackupPoms=false
git clone https://github.com/anthony-gascon/mvnrepo.git
cd /tmp/mvnrepo
mvn install -DlocalRepositoryPath=. -DcreateChecksum=true
git add -A . && git commit -m "released version $1"
git push

