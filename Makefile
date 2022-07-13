ifdef CIRCLE_PR_NUMBER
GIT_BRANCH=pullrequest-$(CIRCLE_PR_NUMBER)
else
GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
endif
ifdef CIRCLE_TAG
GIT_BRANCH=$(CIRCLE_TAG)
endif
$(info GIT_BRANCH=$(GIT_BRANCH))

all: build

installJVMPlugin:
	OS=bionic /bin/bash ./Install_JVMPlugin.sh

build: installJVMPlugin
	mvn -Dmaven.test.skip=true package
	mkdir -p ./artifacts/snapshot/
	mkdir -p ./artifacts/release/
	version=$(shell java -cp ./target/rghibernate-jar-with-dependencies.jar:./bin/RedisGears_JVMPlugin/gears_runtime/target/gear_runtime-jar-with-dependencies.jar com.redislabs.WriteBehind version) && cp ./target/rghibernate-jar-with-dependencies.jar ./artifacts/release/rghibernate-$(GIT_BRANCH)-jar-with-dependencies.jar && cp ./target/rghibernate-jar-with-dependencies.jar ./artifacts/snapshot/rghibernate-$(GIT_BRANCH)-jar-with-dependencies.jar

run: build
	/bin/bash ./run.sh

tests: build
	cd pytest; python3 -m RLTest --clear-logs -s
