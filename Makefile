#OS=$(shell ./deps/readies/bin/platform --osnick)
OS=bionic
$(info OS=$(OS))

all: build
	
build:
	mvn -Dmaven.test.skip=true package

installRedisGears:
	OS=$(OS) /bin/bash ./Install_RedisGears.sh

installJVMPlugin:
	OS=$(OS) /bin/bash ./Install_JVMPlugin.sh

install: build installRedisGears installJVMPlugin

run: install
	/bin/bash ./run.sh

tests: install
	cd pytest; python3 -m RLTest --clear-logs -s
