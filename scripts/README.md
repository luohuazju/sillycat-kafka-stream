1640723333977
Create Topic

```
bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic events_local_file --bootstrap-server centos7-master:9092,centos7-worker1:9092,centos7-worker2:9092
```

List Topic

```
bin/kafka-topics.sh --list --bootstrap-server centos7-master:9092,centos7-worker1:9092,centos7-worker2:9092
```

Send Events

```
bin/kafka-console-producer.sh --broker-list centos7-master:9092,centos7-worker1:9092,centos7-worker2:9092 --topic events_local_file
```

Sample JSON events

```
{"name":"carl", "age": 39, "action:": "create", "content": "1"， "eventTime": 1640723333977 }
```

