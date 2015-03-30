from fabric.api import *

def start():
    run('JAVA_HOME=~/fab/java nohup ~/fab/cassandra/bin/cassandra')

def stop():
    run('pkill -9 -f "java.*org.apache.*.CassandraDaemon"')