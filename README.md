[![license](https://img.shields.io/github/license/RedisGears/rghibernate.svg)](https://github.com/RedisGears/rghibernate/blob/master/LICENSE)
[![release](https://img.shields.io/github/release/RedisGears/rghibernate.svg?sort=semver)](https://github.com/RedisGears/rghibernate/latest)
[![CircleCI](https://circleci.com/gh/RedisGears/rghibernate/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGears/rghibernate/tree/master)
[![Codecov](https://codecov.io/gh/RedisGears/rghibernate/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisGears/rghibernate)
[![Known Vulnerabilities](https://snyk.io/test/github/RedisGears/rghibernate/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/RedisGears/rghibernate?targetFile=pom.xml)

#  rghibernate

[![Forum](https://img.shields.io/badge/Forum-RedisGears-blue)](https://forum.redislabs.com/c/modules/redisgears)
[![Gitter](https://badges.gitter.im/RedisLabs/RedisGears.svg)](https://gitter.im/RedisLabs/RedisGears?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

RedisGears Hibernate based write-behind

### Snapshots
```xml
  <repositories>
    <repository>
      <id>snapshots-repo</id>
      <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
  </repositories>
```

and

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>rghibernate</artifactId>
      <version>0.0.3-SNAPSHOT</version>
    </dependency>
  </dependencies>
```


### Getting started
* Change ./src/test/resources/hibernate.cfg.xml according to your database
* Creating a mapping xml files match hibernate format (look at ./src/test/resources/Student.hbm.xml) for example
* Deploy rghibernate to RedisGears

```bash
python3 ./gears.py <mapping_file_1> [<mapping_file_2> ...]

```
