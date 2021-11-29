if [ -z ${OS+x} ]
then
      echo "os is not set"
      exit 1
fi

echo "installing jvm plugin for :" $OS

WORK_DIR=./bin/RedisGears_JVMPlugin/
JVM_PLUGIN_FILE=gears-jvm.linux-$OS-x64.master.tgz
JVM_PLUGIN_S3_PATH=http://redismodules.s3.amazonaws.com/redisgears_plugins/jvm_plugin/snapshots/$JVM_PLUGIN_FILE

mkdir -p $WORK_DIR

if [ -f "$WORK_DIR$JVM_PLUGIN_FILE" ]; then
    echo "Skiping jvm plugin download"
else 
    echo "Download jvm plugin"
    wget -P $WORK_DIR $JVM_PLUGIN_S3_PATH
    tar -C $WORK_DIR -xvf $WORK_DIR$JVM_PLUGIN_FILE
fi
