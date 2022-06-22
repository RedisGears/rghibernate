[![license](https://img.shields.io/github/license/RedisGears/rghibernate.svg)](https://github.com/RedisGears/rghibernate/blob/master/LICENSE)
[![release](https://img.shields.io/github/release/RedisGears/rghibernate.svg?sort=semver)](https://github.com/RedisGears/rghibernate/latest)
[![CircleCI](https://circleci.com/gh/RedisGears/rghibernate/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGears/rghibernate/tree/master)
[![Codecov](https://codecov.io/gh/RedisGears/rghibernate/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisGears/rghibernate)
[![Known Vulnerabilities](https://snyk.io/test/github/RedisGears/rghibernate/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/RedisGears/rghibernate?targetFile=pom.xml)

#  rghibernate

[![Forum](https://img.shields.io/badge/Forum-RedisGears-blue)](https://forum.redislabs.com/c/modules/redisgears)
[![Gitter](https://badges.gitter.im/RedisLabs/RedisGears.svg)](https://gitter.im/RedisLabs/RedisGears?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

RedisGears Hibernate based write/read-behind/through

## Write-Behind/Through
Write-Behind/Through Implementation, that mainly consists of two RedisGears functions and operates as follows:
1. A write operation to a Redis Hash key triggers the execution of a RedisGears function.
1. That RedisGears function reads the data from the Hash and writes into a Redis Stream.
1. Another RedisGears function is executed asynchronously in the background and writes the changes to the target database.

### The motivation for using a Redis Stream
The use of a Redis Stream in the _Write Behind_ recipe implementation is to ensure the persistence of captured changes while mitigating the performance penalty associated with shipping them to the target database.

The recipe's first RedisGears function is registered to run synchronously, which means that the function runs in the same main Redis thread in which the command was executed. This mode of execution is needed so changes events are recorded in order and to eliminate the possibility of losing events in case of failure.

Applying the changes to the target database is usually much slower, effectively excluding the possibility of doing that in the main thread. The second RedisGears function is executed asynchronously on batches and in intervals to do that.

The Redis Stream is the channel through which both of the recipe's parts communicate, where the changes are persisted in order synchronously and are later processed in the background asynchronously.

## Read-Through
Read-Through implementation that mainly register on key-miss notification and go fetch the relevant data on such miss. The client is blocked until the data is retrieve and then unblocked and gets the relevant data.

## Quick Start
### Prerequisites
* Install [Redis](https://redis.io/) with [RedisGears](https://oss.redislabs.com/redisgears/) and [JVMPlugin](https://github.com/RedisGears/JVMPlugin) (You can easily start a docker with RedisGears and JVMPlugin like this: `docker run redisfab/redisgearsjvm:edge`).
* Install maven
* Install build essential

### Build
Afer downloading rghibernate, run that command in the downloaded folder to get the submodules:
```
git submodule update --init --recursive
```
Execute `make build`, when finished you will find the compile jar `artifacts/release/rghibernate-<version>-jar-with-dependencies.jar`

### Test

Tests rely on a mysql server configured, to allow connections from the user *demouser* with the password *Password123!*.

### Example
First we need to upload the rghibernate jar to RedisGears like this:
```
redis-cli -x RG.JEXECUTE com.redislabs.WriteBehind < ./artifacts/release/rghibernate-0.1.1-jar-with-dependencies.jar
```

In this example we will configure a ReadThroug and WriteBehind into a Student database with 4 columns on mysql database. Our connector xml looks like this:
```xml
<!DOCTYPE hibernate-configuration PUBLIC
        "-//Hibernate/Hibernate Configuration DTD 3.0//EN"
  "http://www.hibernate.org/dtd/hibernate-configuration-3.0.dtd">
        
<hibernate-configuration>
  <session-factory>
    <!-- JDBC Database connection settings -->
    <property name="connection.driver_class">org.mariadb.jdbc.Driver</property>
    <property name="connection.url">jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true&amp;useSSL=false</property>
    <property name="connection.username">user</property>
    <property name="connection.password">pass</property>
    <!-- JDBC connection pool settings ... using built-in test pool -->
    <property name="connection.pool_size">1</property>
    <!-- Echo the SQL to stdout -->
    <property name="show_sql">false</property>
    <!-- Set the current session context -->
    <property name="current_session_context_class">thread</property>
    <!-- Drop and re-create the database schema on startup -->
    <property name="hbm2ddl.auto">update</property>
    <!-- dbcp connection pool configuration -->
    <property name="hibernate.dbcp.initialSize">5</property>
    <property name="hibernate.dbcp.maxTotal">20</property>
    <property name="hibernate.dbcp.maxIdle">10</property>
    <property name="hibernate.dbcp.minIdle">5</property>
    <property name="hibernate.dbcp.maxWaitMillis">-1</property>
  </session-factory>
</hibernate-configuration>
```
We will upload our connector like this:
```
> redis-cli -x RG.TRIGGER SYNC.REGISTERCONNECTOR mysql 1000 10 5 < src/test/resources/mysql_hibernate.cfg.xml 
1) "OK"
```
Now we need to upload our mapping xml for WriteBehind (we call it a source that tells how to map hashes into the backend database). Our mapping xml looks like this:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<hibernate-mapping xmlns="http://www.hibernate.org/xsd/hibernate-mapping"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.hibernate.org/xsd/hibernate-mapping
    http://www.hibernate.org/xsd/hibernate-mapping/hibernate-mapping-4.0.xsd">

  <class entity-name="Student" table="student">
          <tuplizer entity-mode="dynamic-map" class="org.hibernate.tuple.entity.DynamicMapEntityTuplizer"/>
          <id name="id" type="integer" length="50" column="id"/>
          <property name="firstName" column="first_name" type="string"/>
          <property name="lastName" column="last_name" type="string"/>
          <property name="email" column="email" type="string" not-null="true"/>
          <property name="age" column="age" type="integer"/>
  </class>

</hibernate-mapping>
```
Notice, the `entity-name` will be used as key prefix on the Redis hash and the id will be the value that follow the key prefix. In our example the hash `Student:1` will be mapped to table `student` and the `id` value will be `1` (the `:` is not part of entity name and are added automatically). We will upload our source like this:
```
> redis-cli -x RG.TRIGGER SYNC.REGISTERSOURCE StudentWrite mysql WriteBehind < src/test/resources/Student.hbm.xml 
1) "OK"
```
Last we need to upload our mapping for ReadThrough, we will use the same mapping file and same connector but this time we will specify a ReadThrough policy
```
> redis-cli -x RG.TRIGGER SYNC.REGISTERSOURCE StudentRead mysql ReadThrough 0 < src/test/resources/Student.hbm.xml 
1) "OK"
```
The argument after the ReadThrough policy is the expire value, 0 means no expire.


And we are done, now every time we write a data to Student:* the data will be replicated to mysql, and each time we will try to read a unexisting key with prefix Student:*, the keys will be fetch from mysql.


### Commands list
* SYNC.REGISTERCONNECTOR <connector name> <batch size> <timeout> <retry interval> <connector xml>
Register a new connector
    * connector name - a name you want to give to your connector.
    * batch size - the data are sent to the backend in batches, this values allows to set the batch size.
    * timeout - after this timeout, even if batch size was not reached, the data will be sent to the backend.
    * retry interval - retry interval on error.
    * connector xml - hibernate xml definition of the connector.

Example:
```
> redis-cli -x --host <host> --port <port> RG.TRIGGER SYNC.REGISTERCONNECTOR mysql 1000 10 5 < src/test/resources/mysql_hibernate.cfg.xml 
1) "OK"
```

* SYNC.UNREGISTERCONNECTOR <connector name>
Unregister a connector (notice a connector can not be unregistered if it has sources attached to it)

* SYNC.REGISTERSOURCE <source name> <connector name> <policy> [extra config based on policy] <mapping xml>
    * source name - name you want to give to your source
    * connector name - connector to send the data to
    * policy - WriteBehind/WriteThrough/ReadThrough
        * On WriteThroug the extra argument are WriteTimeout
        * On ReadThrough the extra argument are expire (0 for no expire)
    * mapping xml - hibernate xml definition of the mapping

Example:
```
> redis-cli -x --host <host> --port <port> RG.TRIGGER SYNC.REGISTERSOURCE StudentWrite mysql WriteBehind < src/test/resources/Student.hbm.xml 
1) "OK"
```

* SYNC.UNREGISTERSOURCE <source name>
Unregister a source

 * SYNC.INFO SOURCES
dump all sources, Example:
 ```
 127.0.0.1:6379> RG.TRIGGER SYNC.INFO SOURCES
1)  1) "name"
    2) "StudentWrite"
    3) "registrationId"
    4) 1) "0000000000000000000000000000000000000000-13"
    5) "hashPrefix"
    6) "Student"
    7) "connector"
    8) "mysql"
    9) "idProperty"
   10) "id -> id (type: integer, mandatory: true, isId: true)"
   11) "Mappings"
   12) 1) "firstName -> first_name (type: string, mandatory: false, isId: false)"
       2) "lastName -> last_name (type: string, mandatory: false, isId: false)"
       3) "email -> email (type: string, mandatory: true, isId: false)"
       4) "age -> age (type: integer, mandatory: false, isId: false)"
   13) "steamName"
   14) "_Stream-mysql-641d7873-6ff3-4476-a5bd-ae2fe7050755-{06S}"
   15) "policy"
   16) "writeBehind"
2)  1) "name"
    2) "StudentRead"
    3) "registrationId"
    4) 1) "0000000000000000000000000000000000000000-15"
    5) "hashPrefix"
    6) "Student"
    7) "connector"
    8) "mysql"
    9) "idProperty"
   10) "id -> id (type: integer, mandatory: true, isId: true)"
   11) "Mappings"
   12) 1) "firstName -> first_name (type: string, mandatory: false, isId: false)"
       2) "lastName -> last_name (type: string, mandatory: false, isId: false)"
       3) "email -> email (type: string, mandatory: true, isId: false)"
       4) "age -> age (type: integer, mandatory: false, isId: false)"
   13) "policy"
   14) "readThrough"
   15) "expire"
   16) "0"
 ```

 * SYNC.INFO CONNECTORS
dump all connectors, Example:
 ```
127.0.0.1:6379> RG.TRIGGER SYNC.INFO CONNECTORS
1)  1) "name"
    2) "mysql"
    3) "url"
    4) "jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true&useSSL=false"
    5) "driverClass"
    6) "org.mariadb.jdbc.Driver"
    7) "userName"
    8) "demouser"
    9) "dialect"
   10) (nil)
   11) "uuid"
   12) "641d7873-6ff3-4476-a5bd-ae2fe7050755"
   13) "registrationId"
   14) "0000000000000000000000000000000000000000-11"
   15) "batchSize"
   16) "1000"
   17) "duration"
   18) "10"
   19) "retryInterval"
   20) "5"
   21) "streamName"
   22) "_Stream-mysql-641d7873-6ff3-4476-a5bd-ae2fe7050755-{06S}"
   23) "pendingClients"
   24) "0"
   25) "backlog"
   26) "0"
```
