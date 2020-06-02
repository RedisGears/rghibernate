import time
import redis

conn = redis.Redis()

with open('/home/guy/redisclients/rghibernate/target/rghibernate-0.0.1-SNAPSHOT-jar-with-dependencies.jar', 'rb') as f:
    data = f.read()
    start = time.time()
    res = conn.execute_command('rg.jexecute', 'com.redislabs.WriteBehind', data)
    end = time.time()
    if len(res[1]) > 0:
        for e in res[1]:
            print(e)
    else:
        for r in res[0]:
            print(r)
    print('took : %s' % (end - start))                                           