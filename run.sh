set -x

# redis-server --loadmodule ./bin/RedisGears/redisgears.so CreateVenv 1 pythonInstallationDir ./bin/RedisGears/ \
# 	Plugin ./bin/RedisGears_JVMPlugin/plugin/gears_jvm.so \
# 	JvmOptions "-Djava.class.path=./bin/RedisGears_JVMPlugin/gears_runtime/target/gear_runtime-jar-with-dependencies.jar -Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=y" \
# 	JvmPath ./bin/RedisGears_JVMPlugin/bin/OpenJDK/jdk-11.0.9.1+1/ &

redis-server --loadmodule ./bin/RedisGears/redisgears.so CreateVenv 1 pythonInstallationDir ./bin/RedisGears/ \
	Plugin ./bin/RedisGears_JVMPlugin/plugin/gears_jvm.so \
	JvmOptions "-Djava.class.path=./bin/RedisGears_JVMPlugin/gears_runtime/target/gear_runtime-jar-with-dependencies.jar" \
	JvmPath ./bin/RedisGears_JVMPlugin/bin/OpenJDK/jdk-11.0.9.1+1/ &

 redis-cli ping
 while [  $? != 0 ]; do
 	sleep 1
 	redis-cli ping
 done

redis-cli -x RG.JEXECUTE com.redislabs.WriteBehind < ./target/rghibernate-jar-with-dependencies.jar

wait < <(jobs -p)