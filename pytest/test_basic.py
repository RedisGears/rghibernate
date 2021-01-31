from RLTest import Env
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import time
import subprocess
import docker
import signal
from threading import Thread

class Background(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """

    def doJob(self):
        self.f()
        self.isAlive = False

    def __init__(self, f):
        self.f = f
        self.isAlive = True

    def __enter__(self):
        self.t = Thread(target = self.doJob)
        self.t.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.t.join()

class TimeLimit(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """

    def __init__(self, timeout, env, msg):
        self.timeout = timeout
        self.env = env
        self.msg = msg

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.setitimer(signal.ITIMER_REAL, self.timeout, 0)

    def __exit__(self, exc_type, exc_value, traceback):
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

    def handler(self, signum, frame):
        self.env.assertTrue(False, message=self.msg)
        raise Exception(self.msg)

def Connect():
    ConnectionStr = 'oracle://{user}:{password}@{db}'.format(user='system', password='oracle', db='localhost:1521/xe')
    engine = create_engine(ConnectionStr).execution_options(autocommit=True)
    conn = engine.connect()
    return conn

def GetConnection():
    while True:
        try:
            return Connect() 
        except Exception as e:
            time.sleep(1)

class genericTest:
    def __init__(self, writePolicy, retryInterval=5, timeout=10):
        
        self.client = docker.from_env()
        self.container = self.getDockerContainer()
        self.network = [n for n in self.client.networks.list() if n.name == 'bridge'][0]
        try:
            # in case a test stop in the middle after disconnect the network
            self.connectDockerToNetwork()
        except Exception:
            pass

        self.dbConn = GetConnection()

        self.env = Env(module='../bin/RedisGears/redisgears.so', moduleArgs='CreateVenv 1 pythonInstallationDir ../../bin/RedisGears/ Plugin ../../bin/RedisGears_JVMPlugin/plugin/gears_jvm.so JvmOptions -Djava.class.path=../../bin/RedisGears_JVMPlugin/gears_runtime/target/gear_runtime-jar-with-dependencies.jar JvmPath ../../bin/RedisGears_JVMPlugin/bin/OpenJDK/jdk-11.0.9.1+1/')
        with open('../target/rghibernate-jar-with-dependencies.jar', 'rb') as f:
            self.env.cmd('RG.JEXECUTE', 'com.redislabs.WriteBehind', f.read())

        with open('../src/test/resources/hibernate.cfg.xml', 'rt') as f:
            self.env.cmd('RG.TRIGGER', 'SYNC.REGISTERCONNECTOR', 'oracle_connector', '10', '10', str(retryInterval), f.read())

        with open('../src/test/resources/Student.hbm.xml', 'rt') as f:
            cmd = ['RG.TRIGGER', 'SYNC.REGISTERSOURCE', 'students_src', 'oracle_connector', writePolicy]
            if writePolicy == 'WriteThrough':
                cmd += [str(timeout)]
            cmd += [f.read()]
            self.env.cmd(*cmd)

    def setUp(self):
        # verify all executions are done
        done = False
        while not done:
            executions = self.env.cmd('RG.DUMPEXECUTIONS')
            done = True
            for r in executions:
                if r[3] != b'done' and r[3] != b'aborted':
                    done = False
                    time.sleep(0.1)
                    break
        try:
            self.dbConn.execute(text('delete from student'))
        except Exception:
            pass
        self.env.cmd('flushall')

    def disconnectDockerFromNetwork(self):
        self.network.disconnect(self.container.attrs['Id'])

    def connectDockerToNetwork(self):
        self.network.connect(self.container.attrs['Id'])

    def getDockerContainer(self):
        container = [container for container in self.client.containers.list() if container.attrs['Config']['Image'] == 'quay.io/maksymbilenko/oracle-12c']
        if len(container) == 0:
            print('Starting oracle container')
            process = subprocess.Popen(['/bin/bash', '../install_oracle.sh'], stdout=subprocess.PIPE)
            while len(container) == 0:
                container = [container for container in self.client.containers.list() if container.attrs['Config']['Image'] == 'quay.io/maksymbilenko/oracle-12c']
        else:
            print('Oracle container already running')
    
        return container[0]

class testWriteBehind(genericTest):
    def __init__(self):
        genericTest.__init__(self, 'WriteBehind')

    def testSimpleWriteBehind(self):
        self.env.cmd('hset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')

        result = None
        res = None
        with TimeLimit(10, self.env, 'Failed waiting for data to reach the db'):
            while result is None or res is None:
                time.sleep(0.1)
                try:
                    result = self.dbConn.execute(text('select * from student'))
                    res = result.next()
                except Exception as e:
                    pass

        self.env.assertEqual(res, (1, 'foo', 'bar', 'email', 10))

        self.env.cmd('del', 'Student:1')

        with TimeLimit(10, self.env, 'Failed waiting for data to delete from db'):
            while res is not None:
                time.sleep(0.1)
                result = self.dbConn.execute(text('select * from student'))
                res = None
                try:
                    res = result.next()
                except Exception:
                    pass

    def testSimpleWriteBehind2(self):
        for i in range(100):
            self.env.cmd('hmset', 'Student:%d' % i, 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')

        result = None
        res = None
        with TimeLimit(10, self.env, 'Failed waiting for data to reach the db'):
            while result is None or res is None or res[0] != 100:
                time.sleep(0.1)
                try:
                    result = self.dbConn.execute(text('select count(*) from student'))
                    res = result.next()
                except Exception as e:
                    pass

        self.env.assertEqual(res, (100,))

        for i in range(100):
            self.env.cmd('del', 'Student:%d' % i)
        

        with TimeLimit(10, self.env, 'Failed waiting for data to delete from db'):
            while res is not None:
                time.sleep(0.1)
                result = self.dbConn.execute(text('select * from student'))
                res = None
                try:
                    res = result.next()
                except Exception:
                    pass

    def testStopDBOnTrafic(self):
        for i in range(100):
            self.env.cmd('hmset', 'Student:%d' % i, 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')
            if i == 50:
                self.disconnectDockerFromNetwork()
        
        self.connectDockerToNetwork()

        self.dbConn = GetConnection()
        
        # make sure all data was written
        result = None
        res = None
        with TimeLimit(10, self.env, 'Failed waiting for data to reach the db'):
            while result is None or res is None or res[0] != 100:
                time.sleep(0.1)
                try:
                    result = self.dbConn.execute(text('select count(*) from student'))
                    res = result.next()
                except Exception as e:
                    pass
        self.env.assertEqual(res, (100,))

class testWriteThroughTimeout(genericTest):

    def __init__(self):
        genericTest.__init__(self, 'WriteThrough', retryInterval=1, timeout=1)

    def testWriteThroughTimeout(self):
        self.disconnectDockerFromNetwork()
        with TimeLimit(4, self.env, 'Failed waiting for timeout response'):
            self.env.expect('hset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10').error().contains('Write Timed out')
        self.connectDockerToNetwork()

    def testWriteThroughWithoutTimeout(self):
        self.env.expect('RG.TRIGGER', 'SYNC.REGISTERSOURCE', 'students_src', 'oracle_connector', 'WriteThrough', 'bad timeout', 'xml').error().contains('Could not parse timeout argument')

class testWriteThrough(genericTest):

    def __init__(self):
        genericTest.__init__(self, 'WriteThrough')

    def testSimpleWriteThrough(self):
        self.env.cmd('hset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')

        result = self.dbConn.execute(text('select * from student'))
        res = result.next()

        self.env.assertEqual(res, (1, 'foo', 'bar', 'email', 10))

        self.env.cmd('del', 'Student:1')

        result = self.dbConn.execute(text('select * from student'))

        try:
            result.next()
            self.env.assertTrue(False, message='got results when expecting no results')
        except Exception:
            pass
        
    def testSimpleWriteThrough2(self):
        for i in range(100):
            self.env.cmd('hmset', 'Student:%d' % i, 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')
        
        result = self.dbConn.execute(text('select count(*) from student'))
        res = result.next()

        self.env.assertEqual(res, (100,))

        for i in range(100):
            self.env.cmd('del', 'Student:%d' % i)
        

        result = self.dbConn.execute(text('select * from student'))
        try:
            result.next()
            self.env.assertTrue(False, message='got results when expecting no results')
        except Exception:
            pass

    def testStopDBOnTrafic(self):
        def background():
            for i in range(100):
                self.env.cmd('hmset', 'Student:%d' % i, 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')

            self.dbConn = GetConnection()

            result = self.dbConn.execute(text('select count(*) from student'))
            res = result.next()

            self.env.assertEqual(res, (100,))

        with TimeLimit(60*5, self.env, 'Failed waiting for data to reach the database'):
            with Background(background):
                time.sleep(0.5)
                self.disconnectDockerFromNetwork()
                time.sleep(0.5)
                self.connectDockerToNetwork()

    def testMandatoryValueMissing(self):
        self.env.expect('hmset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'age', '10').error().contains('mandatory "email" value is not set')

    def testBadValueAccordingToSchema(self):
        self.env.expect('hmset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', 'test').error().contains('Failed parsing acheme for field "age"')

    def testBadIdValue(self):
        self.env.expect('hmset', 'Student:test', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', 'test').error().contains('Failed parsing id field "id"')

    def testExtraHashFieldsAreIgnored(self):
        self.env.cmd('hset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10', 'bearth_year', 1999)

        self.env.expect('hget', 'Student:1', 'bearth_year').equal(b'1999')

        result = self.dbConn.execute(text('select * from student'))
        res = result.next()

        self.env.assertEqual(res, (1, 'foo', 'bar', 'email', 10))

    def testHIncrByFloat(self):
        self.env.cmd('hset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')

        self.env.cmd('hincrbyfloat', 'Student:1', 'age', '2')

        result = self.dbConn.execute(text('select * from student'))
        res = result.next()

        self.env.assertEqual(res, (1, 'foo', 'bar', 'email', 12))

    def testHIncr(self):
        self.env.cmd('hset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')

        self.env.cmd('hincrby', 'Student:1', 'age', '2')

        result = self.dbConn.execute(text('select * from student'))
        res = result.next()

        self.env.assertEqual(res, (1, 'foo', 'bar', 'email', 12))

    def testNotMandatoryValue(self):
        self.env.cmd('hset', 'Student:1', 'lastName', 'bar', 'email', 'email', 'age', '10')

        result = self.dbConn.execute(text('select * from student'))
        res = result.next()

        self.env.assertEqual(res, (1, None, 'bar', 'email', 10))

    def testHdel(self):
        self.env.cmd('hset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')

        result = self.dbConn.execute(text('select * from student'))
        res = result.next()

        self.env.assertEqual(res, (1, 'foo', 'bar', 'email', 10))

        self.env.cmd('hdel', 'Student:1', 'firstName')

        result = self.dbConn.execute(text('select * from student'))
        res = result.next()

        self.env.assertEqual(res, (1, None, 'bar', 'email', 10))

    def testHsetnx(self):
        self.env.cmd('hset', 'Student:1', 'lastName', 'bar', 'email', 'email', 'age', '10')

        result = self.dbConn.execute(text('select * from student'))
        res = result.next()

        self.env.assertEqual(res, (1, None, 'bar', 'email', 10))

        self.env.cmd('hsetnx', 'Student:1', 'firstName', 'foo')        

        result = self.dbConn.execute(text('select * from student'))
        res = result.next()

        self.env.assertEqual(res, (1, 'foo', 'bar', 'email', 10))

    def testHdelOnMandatoryField(self):
        self.env.cmd('hset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')

        result = self.dbConn.execute(text('select * from student'))
        res = result.next()

        self.env.assertEqual(res, (1, 'foo', 'bar', 'email', 10))

        self.env.expect('hdel', 'Student:1', 'email').error().contains('mandatory "email" value is not set')

        self.env.cmd('hdel', 'Student:1', 'firstName', 'lastName', 'age')

        result = self.dbConn.execute(text('select * from student'))
        try:
            result.next()
            self.env.assertTrue(False, message='got results when expecting no results')
        except Exception:
            pass

    def reloadingIterator(self):
        yield 1
        self.env.cmd('save')
        self.env.stop()
        self.env.start()
        yield 2

    def testUnregisterSourceAndConnector(self):
        self.env.cmd('RG.TRIGGER', 'SYNC.UNREGISTERSOURCE', 'students_src')
        self.env.cmd('RG.TRIGGER', 'SYNC.UNREGISTERCONNECTOR', 'oracle_connector')

        for _ in self.reloadingIterator():
            self.env.cmd('hset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '10')

            result = self.dbConn.execute(text('select * from student'))
            try:
                result.next()
                self.env.assertTrue(False, message='got results when expecting no results')
            except Exception:
                pass

        # register oracle_connector
        with open('../src/test/resources/hibernate.cfg.xml', 'rt') as f:
            try:
                self.env.cmd('RG.TRIGGER', 'SYNC.REGISTERCONNECTOR', 'oracle_connector', '10', '10', '5', f.read())
            except Exception as e:
                print(e)
                input('stopped')

        # reregister student source
        with open('../src/test/resources/Student.hbm.xml', 'rt') as f:
            try:
                self.env.cmd('RG.TRIGGER', 'SYNC.REGISTERSOURCE', 'students_src', 'oracle_connector', 'WriteThrough', '10', f.read())
            except Exception as e:
                print(e)
                input('stopped')

        for _ in self.reloadingIterator():
            self.env.cmd('hset', 'Student:1', 'firstName', 'foo', 'lastName', 'bar', 'email', 'email', 'age', '11')

            result = self.dbConn.execute(text('select * from student'))
            res = result.next()

            self.env.assertEqual(res, (1, 'foo', 'bar', 'email', 11))

    def testUnregisterConnectorWithSources(self):
        self.env.expect('RG.TRIGGER', 'SYNC.UNREGISTERCONNECTOR', 'oracle_connector').error().contains('Can\'t unregister connector with sources')
