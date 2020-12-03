OS=$(shell ./deps/readies/bin/platform --osnick)
$(info OS=$(OS))

all: build
	
installRedisGears:
	OS=$(OS) /bin/bash ./Install_RedisGears.sh

installJVMPlugin:
	OS=bionic /bin/bash ./Install_JVMPlugin.sh

build: installRedisGears installJVMPlugin
	mvn -Dmaven.test.skip=true package

run: build
	/bin/bash ./run.sh

tests: build
	cd pytest; python3 -m RLTest --clear-logs -s
