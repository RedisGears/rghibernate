#!/bin/bash
[[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e
# https://github.com/MaksymBilenko/docker-oracle-12c
# ORACLE_IMAGE=orangehrm/oracle-xe-11g
ORACLE_IMAGE=quay.io/maksymbilenko/oracle-12c
ORACLE=oracle
rm -f /tmp/oracle.cid
docker stop $ORACLE > /dev/null 2>&1 || true
docker rm $ORACLE > /dev/null 2>&1 || true
sleep 3
docker run --name=$ORACLE --cidfile /tmp/oracle.cid --rm -d -p 1521:1521 -p 8080:8080 $ORACLE_IMAGE
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $ORACLE> /tmp/oracle.ip
cat /tmp/oracle.ip
cat <<- 'EOF' > /tmp/setup-sqlplus
	yum install -y libaio
	rpm -i https://download.oracle.com/otn_software/linux/instantclient/195000/oracle-instantclient19.5-basic-19.5.0.0.0-1.x86_64.rpm
	rpm -i https://download.oracle.com/otn_software/linux/instantclient/195000/oracle-instantclient19.5-sqlplus-19.5.0.0.0-1.x86_64.rpm
	rpm -ivh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
	yum install -y rlwrap
EOF
docker cp /tmp/setup-sqlplus $ORACLE:/tmp/setup-sqlplus
docker exec $ORACLE bash -c ". /tmp/setup-sqlplus"
exit 0
