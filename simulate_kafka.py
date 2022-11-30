

## Will generate multiple payloads to kafka broker on the given server
from kafka import KafkaProducer
bootstrap_servers = ['172.18.0.4:9092']
topicName = 'cdc_test_topics'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
payloads = [r"""
	{"payload": {
		"before": {
			"id": {
				"value": "c7392418-5e9a-4af4-8166-211e30b4de93",
				"set": true
			},
			"status": null,
			"status_metadata": null,
			"creator": null,
			"created": null,
			"creator_type": null,
			"updater": null,
			"updated": null,
			"updater_type": null
		},
		"after": null,
		"source": {
			"version": "1.7.0.13-BETA",
			"connector": "yugabytedb",
			"name": "dbserver1",
			"ts_ms": -4258763232015,
			"snapshot": "false",
			"db": "yugabyte",
			"sequence": "[\"0:0::0:0\",\"1:367::0:0\"]",
			"schema": "public",
			"table": "customer",
			"txId": "",
			"lsn": "1:367::0:0",
			"xmin": null
		},
		"op": "d",
		"ts_ms": 1669795724766,
		"transaction": null
	}
""",r"""
	{"payload": {
		"before": null,
		"after": {
			"id": {
				"value": "c95dc377-c5cc-4f8a-94aa-605faff9121c",
				"set": true
			},
			"status": {
				"value": "kfxrnNsqt",
				"set": true
			},
			"status_metadata": {
				"value": null,
				"set": true
			},
			"creator": null,
			"created": null,
			"creator_type": null,
			"updater": {
				"value": "e2e10c3b-9830-4612-958a-211beb6e282e",
				"set": true
			},
			"updated": {
				"value": 1669806524287,
				"set": true
			},
			"updater_type": {
				"value": "SYSTEM",
				"set": true
			}
		},
		"source": {
			"version": "1.7.0.13-BETA",
			"connector": "yugabytedb",
			"name": "dbserver1",
			"ts_ms": -4258764640265,
			"snapshot": "false",
			"db": "yugabyte",
			"sequence": "[\"0:0::0:0\",\"2:344::0:0\"]",
			"schema": "public",
			"table": "customer",
			"txId": "",
			"lsn": "2:344::0:0",
			"xmin": null
		},
		"op": "u",
		"ts_ms": 1669795724772,
		"transaction": null
	}
""",
r"""
	{"payload": {
		"before": null,
		"after": {
			"id": {
				"value": "260f50de-f58d-4240-be57-663b29f11a73",
				"set": true
			},
			"status": {
				"value": "zFUmFFObM",
				"set": true
			},
			"status_metadata": {
				"value": null,
				"set": true
			},
			"creator": null,
			"created": null,
			"creator_type": null,
			"updater": {
				"value": "e47c1c7b-da4f-4540-ad7b-b9f83f986874",
				"set": true
			},
			"updated": {
				"value": 1669806524320,
				"set": true
			},
			"updater_type": {
				"value": "SYSTEM",
				"set": true
			}
		},
		"source": {
			"version": "1.7.0.13-BETA",
			"connector": "yugabytedb",
			"name": "dbserver1",
			"ts_ms": -4258764508808,
			"snapshot": "false",
			"db": "yugabyte",
			"sequence": "[\"0:0::0:0\",\"2:345::0:0\"]",
			"schema": "public",
			"table": "customer",
			"txId": "",
			"lsn": "2:345::0:0",
			"xmin": null
		},
		"op": "u",
		"ts_ms": 1669795724772,
		"transaction": null
	}
""",
r"""
	{"payload": {
		"before": {
			"id": {
				"value": "e0640808-c8f9-4d45-95a6-c834c2fb246a",
				"set": true
			},
			"status": null,
			"status_metadata": null,
			"creator": null,
			"created": null,
			"creator_type": null,
			"updater": null,
			"updated": null,
			"updater_type": null
		},
		"after": null,
		"source": {
			"version": "1.7.0.13-BETA",
			"connector": "yugabytedb",
			"name": "dbserver1",
			"ts_ms": -4258763893863,
			"snapshot": "false",
			"db": "yugabyte",
			"sequence": "[\"0:0::0:0\",\"2:346::0:0\"]",
			"schema": "public",
			"table": "customer",
			"txId": "",
			"lsn": "2:346::0:0",
			"xmin": null
		},
		"op": "d",
		"ts_ms": 1669795724772,
		"transaction": null
	}
}
""",
r"""
{
	"payload": {
		"before": {
			"id": {
				"value": "b67886f5-e64d-42a1-bf83-fcbbd5001d47",
				"set": true
			},
			"status": null,
			"status_metadata": null,
			"creator": null,
			"created": null,
			"creator_type": null,
			"updater": null,
			"updated": null,
			"updater_type": null
		},
		"after": null,
		"source": {
			"version": "1.7.0.13-BETA",
			"connector": "yugabytedb",
			"name": "dbserver1",
			"ts_ms": -4258763692835,
			"snapshot": "false",
			"db": "yugabyte",
			"sequence": "[\"0:0::0:0\",\"1:338::0:0\"]",
			"schema": "public",
			"table": "customer",
			"txId": "",
			"lsn": "1:338::0:0",
			"xmin": null
		},
		"op": "d",
		"ts_ms": 1669795724779,
		"transaction": null
	}
}

"""]
for payload in payloads:
    producer.send('cdc_test_topics',payload.encode('utf-8')).get(timeout=10) 
