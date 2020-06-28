import time
import redis

#conn = redis.Redis('redis-14065.test18880736.cto.redislabs.com', 14065, password='Password123')
conn = redis.Redis()

with open('./target/rghibernate-0.0.3-SNAPSHOT-jar-with-dependencies.jar', 'rb') as f:
    data = f.read()
    start = time.time()
    connectionXml = open('./src/test/resources/hibernate.cfg.xml', 'rt').read()
    mappingXml = open('./src/test/resources/Student.hbm.xml', 'rt').read()
    print(connectionXml)
    res = conn.execute_command('rg.jexecute', 'com.redislabs.WriteBehind', data, connectionXml, mappingXml)
    end = time.time()
    print(res)
    print('took : %s' % (end - start))                                           
