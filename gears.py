import redis
import click
import os

class Colors(object):
    @staticmethod
    def Cyan(data):
        return '\033[36m' + data + '\033[0m'

    @staticmethod
    def Yellow(data):
        return '\033[33m' + data + '\033[0m'

    @staticmethod
    def Bold(data):
        return '\033[1m' + data + '\033[0m'

    @staticmethod
    def Bred(data):
        return '\033[31;1m' + data + '\033[0m'

    @staticmethod
    def Gray(data):
        return '\033[30;1m' + data + '\033[0m'

    @staticmethod
    def Lgray(data):
        return '\033[30;47m' + data + '\033[0m'

    @staticmethod
    def Blue(data):
        return '\033[34m' + data + '\033[0m'

    @staticmethod
    def Green(data):
        return '\033[32m' + data + '\033[0m'

@click.group()
def rghibernate():
    pass

def create_connection(host, port, password, decode_responses=True):
    global args
    try:
        r = redis.Redis(host, port, password=password, decode_responses=decode_responses)
        r.ping()
    except Exception as e:
        print(Colors.Bred('Cannot connect to Redis. Aborting (%s)' % str(e)))
        exit(1)
    return r

@rghibernate.command(help='Upload rghibernate recipe to RedisGears (MAPPING is a list of xml mappings in hibernate format)')
@click.option('--host', default='localhost', help='Redis host to connect to')
@click.option('--port', default=6379, type=int, help='Redis port to connect to')
@click.option('--password', default=None, help='Redis password')
@click.option('--rghibernate-jar', default='./target/rghibernate-0.0.3-SNAPSHOT-jar-with-dependencies.jar', help='Path to rghibernate jar file')
def upload_recipe(host, port, password, rghibernate_jar):
    conn = create_connection(host, port, password)

    if not os.path.exists(rghibernate_jar):
        print(Colors.Bred('rghibernate jar file does not exists'))
        exit(1)

    with open(rghibernate_jar, 'rb') as f:
        data = f.read()
        try:
            res = conn.execute_command('rg.jexecute', 'com.redislabs.WriteBehind', data)
        except Exception as e:
            print(Colors.Bred('Failed executing jexecute command. Aborting (%s)' % str(e).replace('|', '\n')))
            exit(1)
        print(Colors.Green(res))


def main():
    rghibernate()

if __name__ == '__main__':
    rghibernate()
