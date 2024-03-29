#!/bin/bash

# run this as root

[[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

if [ ! -z $(command -v apt-get) ]; then
	apt-get -qq update
	apt-get install -y ca-certificates wget curl unzip
	apt-get install -y libaio1 rlwrap

	rm -rf /opt/oracle
	mkdir /opt/oracle
	cd /opt/oracle

	wget -q https://download.oracle.com/otn_software/linux/instantclient/195000/instantclient-basic-linux.x64-19.5.0.0.0dbru.zip
	wget -q https://download.oracle.com/otn_software/linux/instantclient/195000/instantclient-sqlplus-linux.x64-19.5.0.0.0dbru.zip
	unzip instantclient-basic-linux.x64-19.5.0.0.0dbru.zip
	unzip instantclient-sqlplus-linux.x64-19.5.0.0.0dbru.zip
	rm -f *.zip

	echo /opt/oracle/instantclient_19_5 > /etc/ld.so.conf.d/oracle-instantclient.conf
	ldconfig
	# export PATH=$PATH:/opt/oracle/instantclient_19_5
	
elif [ ! -z $(command -v yum) ]; then
	yum install -y ca-certificates libaio
	rpm -i https://download.oracle.com/otn_software/linux/instantclient/195000/oracle-instantclient19.5-basic-19.5.0.0.0-1.x86_64.rpm
	rpm -i https://download.oracle.com/otn_software/linux/instantclient/195000/oracle-instantclient19.5-sqlplus-19.5.0.0.0-1.x86_64.rpm
	rpm -ivh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm || true
	yum install -y rlwrap

else
	echo "%make love"
	echo "Make:  Don't know how to make love.  Stop."
	exit 1
fi

echo "export PATH=$PATH:/opt/oracle/instantclient_19_5" > /etc/profile.d/oracle.sh
exit 0

