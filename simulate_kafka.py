"""Will generate multiple payloads to kafka broker on the given server"""
import json
import random
import uuid
from datetime import datetime
from typing import Dict, List

from kafka import KafkaProducer

from streaming.config import kafka_server

BOOTSTRAP_SERVER = [kafka_server]
TOPICNAME = "cdc_test_topics"
PRODUCER = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
NUMBER_OF_PAYLOAD = 50
TOTAL_ITERATION = 1


def static_simulation() -> List[str]:
    """
    This is just legacy function. Serving no purpose. Will remove in the future.

    """
    payloads = [
        r"""
		{"payload": {
			"before": null,
			"after": {
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
			"op": "c",
			"ts_ms": 1669795724765,
			"transaction": null
		}}
		""",
        r"""
		{"payload": {
			"before": null,
			"after": {
				"id": {
					"value": "1947",
					"set": true
				},
				"value":null
			},
			"source": {
				"version": "1.7.0.13-BETA",
				"connector": "yugabytedb",
				"name": "dbserver1",
				"ts_ms": -4258763232015,
				"snapshot": "false",
				"db": "yugabyte",
				"sequence": "[\"0:0::0:0\",\"1:367::0:0\"]",
				"schema": "public",
				"table": "Transaction",
				"txId": "",
				"lsn": "1:367::0:0",
				"xmin": null
			},
			"op": "c",
			"ts_ms": 1669795724769,
			"transaction": null
		}}
		""",
        r"""
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
		}}
	""",
        r"""
		{"payload": {
			"before": null,
			"after": {
				"id": {
					"value": "1947",
					"set": true
				},
				"value":{
					"value":1000,
					"set":true

				}
			},
			"source": {
				"version": "1.7.0.13-BETA",
				"connector": "yugabytedb",
				"name": "dbserver1",
				"ts_ms": -4258763232015,
				"snapshot": "false",
				"db": "yugabyte",
				"sequence": "[\"0:0::0:0\",\"1:367::0:0\"]",
				"schema": "public",
				"table": "Transaction",
				"txId": "",
				"lsn": "1:367::0:0",
				"xmin": null
			},
			"op": "u",
			"ts_ms": 1669795724772,
			"transaction": null
		}}
		""",
        r"""
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
		}}
	""",
        r"""
		{"payload": {
			"before": null,
			"after": {
				"id": {
					"value": "1942",
					"set": true
				},
				"value":{
					"value":10000,
					"set":true

				}
			},
			"source": {
				"version": "1.7.0.13-BETA",
				"connector": "yugabytedb",
				"name": "dbserver1",
				"ts_ms": -4258763232015,
				"snapshot": "false",
				"db": "yugabyte",
				"sequence": "[\"0:0::0:0\",\"1:367::0:0\"]",
				"schema": "public",
				"table": "Transaction",
				"txId": "",
				"lsn": "1:367::0:0",
				"xmin": null
			},
			"op": "c",
			"ts_ms": 1669795724781,
			"transaction": null
		}}
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
		}}
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
			"before": null,
			"after": {
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
			"op": "u",
			"ts_ms": 1669795724773,
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
	""",
        r"""
	{
		"payload": {
			"before": null,
			"after": {
				"id": {
					"value": "MK213141",
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
			"op": "c",
			"ts_ms": 1669795724779,
			"transaction": null
		}
	}
	""",
        r"""
	{
		"payload": {
			"before": null,
			"after": {
				"id": {
					"value": "MK213141",
					"set": true
				},
				"status": {
					"value": "status_update",
					"set": true
				},
				"status_metadata": {
					"value": "status_metadata",
					"set": true
				},
				"creator": {
					"value": "creator",
					"set": true
				},
				"created": null,
				"creator_type": null,
				"updater": null,
				"updated": null,
				"updater_type": null
			},
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
			"op": "u",
			"ts_ms": 1669795724779,
			"transaction": null
		}
	}
	""",
        r"""{}""",
        r"""null""",
        r"""{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"struct","fields":[{"type":"string","optional":false,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":false,"name":"id","field":"id"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status","field":"status"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status_metadata","field":"status_metadata"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator","field":"creator"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"created","field":"created"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator_type","field":"creator_type"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater","field":"updater"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updated","field":"updated"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater_type","field":"updater_type"}],"optional":true,"name":"dbserver1.public.customer.Value","field":"before"},{"type":"struct","fields":[{"type":"struct","fields":[{"type":"string","optional":false,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":false,"name":"id","field":"id"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status","field":"status"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status_metadata","field":"status_metadata"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator","field":"creator"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"created","field":"created"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator_type","field":"creator_type"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater","field":"updater"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updated","field":"updated"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater_type","field":"updater_type"}],"optional":true,"name":"dbserver1.public.customer.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":false,"field":"schema"},{"type":"string","optional":false,"field":"table"},{"type":"string","optional":true,"field":"txId"},{"type":"string","optional":true,"field":"lsn"},{"type":"int64","optional":true,"field":"xmin"}],"optional":false,"name":"io.debezium.connector.postgresql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.public.customer.Envelope"},"payload":{"before":{"id":{"value":"99923492-6026-45a4-b89c-32d4dde8ccb1","set":true},"status":null,"status_metadata":null,"creator":null,"created":null,"creator_type":null,"updater":null,"updated":null,"updater_type":null},"after":null,"source":{"version":"1.7.0.13-BETA","connector":"yugabytedb","name":"dbserver1","ts_ms":-4616124018684,"snapshot":"false","db":"yugabyte","sequence":"[null,\"4:36::0:0\"]","schema":"public","table":"customer","txId":"","lsn":"4:36::0:0","xmin":null},"op":"d","ts_ms":1669708957938,"transaction":null}}""",
        r"""{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"struct","fields":[{"type":"string","optional":false,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":false,"name":"id","field":"id"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status","field":"status"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status_metadata","field":"status_metadata"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator","field":"creator"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"created","field":"created"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator_type","field":"creator_type"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater","field":"updater"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updated","field":"updated"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater_type","field":"updater_type"}],"optional":true,"name":"dbserver1.public.customer.Value","field":"before"},{"type":"struct","fields":[{"type":"struct","fields":[{"type":"string","optional":false,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":false,"name":"id","field":"id"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status","field":"status"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status_metadata","field":"status_metadata"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator","field":"creator"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"created","field":"created"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator_type","field":"creator_type"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater","field":"updater"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updated","field":"updated"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater_type","field":"updater_type"}],"optional":true,"name":"dbserver1.public.customer.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":false,"field":"schema"},{"type":"string","optional":false,"field":"table"},{"type":"string","optional":true,"field":"txId"},{"type":"string","optional":true,"field":"lsn"},{"type":"int64","optional":true,"field":"xmin"}],"optional":false,"name":"io.debezium.connector.postgresql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.public.customer.Envelope"},"payload":{"before":null,"after":{"id":{"value":"99923492-6026-45a4-b89c-32d4dde8ccb1","set":true},"status":null,"status_metadata":null,"creator":null,"created":null,"creator_type":null,"updater":null,"updated":null,"updater_type":null},"source":{"version":"1.7.0.13-BETA","connector":"yugabytedb","name":"dbserver1","ts_ms":-4616124018684,"snapshot":"false","db":"yugabyte","sequence":"[null,\"4:36::0:0\"]","schema":"public","table":"customer","txId":"","lsn":"4:36::0:0","xmin":null},"op":"u","ts_ms":1669708957938,"transaction":null}}""",
        r"""{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"struct","fields":[{"type":"string","optional":false,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":false,"name":"id","field":"id"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status","field":"status"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status_metadata","field":"status_metadata"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator","field":"creator"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"created","field":"created"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator_type","field":"creator_type"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater","field":"updater"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updated","field":"updated"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater_type","field":"updater_type"}],"optional":true,"name":"dbserver1.public.customer.Value","field":"before"},{"type":"struct","fields":[{"type":"struct","fields":[{"type":"string","optional":false,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":false,"name":"id","field":"id"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status","field":"status"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"status_metadata","field":"status_metadata"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator","field":"creator"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"created","field":"created"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"creator_type","field":"creator_type"},{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Uuid","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater","field":"updater"},{"type":"struct","fields":[{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updated","field":"updated"},{"type":"struct","fields":[{"type":"string","optional":true,"field":"value"},{"type":"boolean","optional":false,"field":"set"}],"optional":true,"name":"updater_type","field":"updater_type"}],"optional":true,"name":"dbserver1.public.customer.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":false,"field":"schema"},{"type":"string","optional":false,"field":"table"},{"type":"string","optional":true,"field":"txId"},{"type":"string","optional":true,"field":"lsn"},{"type":"int64","optional":true,"field":"xmin"}],"optional":false,"name":"io.debezium.connector.postgresql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.public.customer.Envelope"},"payload":{"before":null,"after":{"id":{"value":"99923492-6026-45a4-b89c-32d4dde8ccb1","set":true},"status":{"value":"status has been changed","set":true},"status_metadata":{"value":"information","set":true},"creator":{"value":"Mohammad","set":true},"created":null,"creator_type":null,"updater":null,"updated":null,"updater_type":null},"source":{"version":"1.7.0.13-BETA","connector":"yugabytedb","name":"dbserver1","ts_ms":-4616124018684,"snapshot":"false","db":"yugabyte","sequence":"[null,\"4:36::0:0\"]","schema":"public","table":"customer","txId":"","lsn":"4:36::0:0","xmin":null},"op":"u","ts_ms":1669708957938,"transaction":null}}""",
    ]

    return payloads


def random_simulation(number_of_payloads: int) -> List[str]:
    """
    This simulates the desired number of cdc payloads coming from yogabyte to kafka cluster

    Parameters:
    -------------
    number_of_payloads(int): The numbers of payloads messages you to generate for testing

    Returns:
    -------------
    payloads(list): A list of payloads


    """
    payloads = []
    set_ids = set()
    operations = ["c", "u", "d"]
    random_statuses = ["valid", "invalid", "pending"]
    random_status_metadata = [
        "Valid: Going Good",
        "Invalid: Going Bad",
        "Pending: Need to take a decision",
    ]
    random_creator = ["Mohammad", "Oscar", "Carl"]

    random_creator_type = ["Large", "Small", "Medium"]
    random_updater = ["Mohammad", "Oscar", "Carl"]
    random_updater_type = ["Large", "Small", "Medium"]

    for i in range(number_of_payloads):
        dt = datetime.now()
        current_operation = operations[random.randint(0, len(operations) - 1)]

        current_statuses = random_statuses[random.randint(
            0, len(random_statuses) - 1)]

        current_status_metadata = random_status_metadata[
            random.randint(0, len(random_status_metadata) - 1)
        ]

        current_creator = random_creator[random.randint(
            0, len(random_creator) - 1)]
        current_created = int(round(dt.timestamp() * 1000))

        current_creator_type = random_creator_type[random.randint(
            0, len(random_creator_type) - 1)]

        current_updater = random_updater[random.randint(
            0, len(random_updater) - 1)]
        current_updated = int(round(dt.timestamp() * 1000))
        current_updater_type = random_updater_type[random.randint(
            0, len(random_updater_type) - 1)]
        before_payload = {}
        after_payload = {
            "id": {"value": "", "set": True},
            "status": {"value": current_statuses, "set": True},
            "status_metadata": {"value": current_status_metadata, "set": True},
            "creator": {"value": current_creator, "set": True},
            "created": {"value": current_created, "set": True},
            "creator_type": {"value": current_creator_type, "set": True},
            "updater": {"value": current_updater, "set": True},
            "updated": {"value": current_updated, "set": True},
            "updater_type": {"value": current_updater_type, "set": True},
        }

        before_payload = {
            "id": {"value": "", "set": True},
            "status": None,
            "status_metadata": None,
            "creator": None,
            "created": None,
            "creator_type": None,
            "updater": None,
            "updated": None,
            "updater_type": None,
        }
        if current_operation == "c":
            new_id = str(uuid.uuid1())
            set_ids.add(new_id)
            after_payload["id"]["value"] = new_id
            before_payload = None
        elif current_operation == "u" or current_operation == "d":
            if set_ids == set():
                print("No ID in the database")
                continue
            else:
                random_id_to_update_delete = list(set_ids)[
                    random.randint(0, len(list(set_ids)) - 1)
                ]
                if current_operation == "u":
                    after_payload["id"]["value"] = random_id_to_update_delete
                    before_payload = None
                else:
                    after_payload = None
                    before_payload["id"]["value"] = random_id_to_update_delete

        print(f"*** Payload {i+1} was generated ***")
        final_payload = {
            "payload": {
                "before": before_payload,
                "after": after_payload,
                "source": {
                    "version": "1.7.0.13-BETA",
                    "connector": "yugabytedb",
                    "name": "dbserver1",
                    "ts_ms": -4258763232015,
                    "snapshot": "false",
                    "db": "yugabyte",
                    "sequence": '["0:0::0:0","1:367::0:0"]',
                    "schema": "public",
                    "table": "customer",
                    "txId": "",
                    "lsn": "1:367::0:0",
                    "xmin": None,
                },
                "op": current_operation,
                "ts_ms": current_created,
                "transaction": None,
            }
        }
        json_string = json.dumps(final_payload)
        json_string = rf"""{json_string}"""
        payloads.append(json_string)
    return payloads


if __name__ == "__main__":
    num_iteration = 1

    while num_iteration <= TOTAL_ITERATION:
        payloads = random_simulation(NUMBER_OF_PAYLOAD)
        for payload in payloads:
            PRODUCER.send("cdc_test_topics", payload.encode(
                "utf-8")).get(timeout=10)

        num_iteration += 1
        print(f"*** Number of Iterations: {num_iteration} ***")
