"""Will generate multiple payloads to kafka broker on the given server"""
import argparse
import json
import os
import random
import uuid
from datetime import datetime
from typing import Dict, List, Tuple, Union

from kafka import KafkaProducer

from streaming.config import delta_lake_schemas, kafka_server, topic_names

parser = argparse.ArgumentParser(
    description="Simulate Streaming Job from Kafka")
parser.add_argument(
    "-p", "--payloads", default="50", help="Number of payloads you need to send in each iteration"
)
parser.add_argument("-i", "--iterations", default="1", help="Total Iterations")
args = parser.parse_args()

BOOTSTRAP_SERVER = [kafka_server]
PRODUCER = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
NUMBER_OF_PAYLOAD = int(args.payloads)
TOTAL_ITERATION = int(args.iterations)


def static_simulation() -> List[str]:
    """
    This is just legacy function. Serving no purpose. Will remove in the future.

    """
    static_payloads = [
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

    return static_payloads


def generate_field_values(
    table_fields: List[Dict], set_ids: set()
) -> Union[Tuple[Dict, Dict, str, str], bool]:
    """

            This generates after and before payload values for a single random message

    Parameters:
    -------------
    table_fields(List): The fields of the table for which you are generating the payload

            set_ids(set): All unique ids that have been generated in the current iteration

    Returns:
    -------------
    payloads(Tuple or Bool): Generates a Tuple of following values
                            - before_payload
                            - after_payload
                            - current_operation
                            - new_id
                            However, if set_ids is empty then returns a boolean value of False





    """
    dt = datetime.now()
    operations = ["c", "u", "d"]
    random_string_values = [
        "valid",
        "invalid",
        "pending",
        "good",
        "bad",
        "lets go",
        "data was given",
        "Mohammad",
        "Oscar",
        "Carl",
    ]
    after_payload, before_payload = {}, {}
    current_operation = operations[random.randint(0, len(operations) - 1)]
    new_id = None
    for field in table_fields["fields"]:
        if field["name"] == "id" and current_operation == "c":
            random_generated_value = str(uuid.uuid1())
            new_id = random_generated_value
        elif field["name"] == "id" and current_operation in ["u", "d"]:
            if set_ids == set():
                return False
            random_generated_value = list(
                set_ids)[random.randint(0, len(list(set_ids)) - 1)]
            new_id = random_generated_value
        elif field["type"] == "string":
            random_generated_value = random_string_values[
                random.randint(0, len(random_string_values) - 1)
            ]
        elif field["type"] == "integer":
            random_generated_value = random.randint(0, 1000)
        elif field["type"] == "float":
            random_generated_value = random.uniform(0, 10000)
        elif field["type"] == "timestamp":
            random_generated_value = int(round(dt.timestamp() * 1000))

        if current_operation in ["c", "u"]:
            before_payload = None
            after_payload[field["name"]] = {
                "value": random_generated_value, "set": True}
        elif current_operation == "d":
            after_payload = None
            before_payload[field["name"]] = (
                {"value": random_generated_value,
                    "set": True} if field["name"] == "id" else None
            )

    return before_payload, after_payload, current_operation, new_id


def random_simulation(number_of_payloads: int, table_name: str, table_fields: Dict) -> List[str]:
    """
    This simulates the desired number of cdc payloads coming from yogabyte to kafka cluster

    Parameters:
    -------------
    number_of_payloads(int): The numbers of payloads messages you to generate for testing

    Returns:
    -------------
    payloads(list): A list of payloads


    """
    final_payloads = []
    set_ids = set()
    for i in range(number_of_payloads):
        returned_values = generate_field_values(table_fields, set_ids)
        if not isinstance(returned_values, tuple):
            print("*** No IDs in the Database ***")
            continue

        set_ids.add(returned_values[3])
        print(f"*** Payload {i+1} was generated for {table_name} Table***")
        final_payload = {
            "payload": {
                "before": returned_values[0],
                "after": returned_values[1],
                "source": {
                    "version": "1.7.0.13-BETA",
                    "connector": "yugabytedb",
                    "name": "dbserver1",
                    "ts_ms": -4258763232015,
                    "snapshot": "false",
                    "db": "yugabyte",
                    "sequence": '["0:0::0:0","1:367::0:0"]',
                    "schema": "public",
                    "table": table_name,
                    "txId": "",
                    "lsn": "1:367::0:0",
                    "xmin": None,
                },
                "op": returned_values[2],
                "ts_ms": int(round(datetime.now().timestamp() * 1000)),
                "transaction": None,
            }
        }

        json_string = json.dumps(final_payload)
        print(json_string)
        json_string = rf"""{json_string}"""
        final_payloads.append(json_string)
    return final_payloads


if __name__ == "__main__":

    num_iteration = 1

    while num_iteration <= TOTAL_ITERATION:
        for table in delta_lake_schemas:
            payloads = random_simulation(
                NUMBER_OF_PAYLOAD, table, delta_lake_schemas[table])
            for payload in payloads:
                PRODUCER.send(topic_names[table], payload.encode(
                    "utf-8")).get(timeout=10)

        num_iteration += 1
        print(f"*** Number of Iterations: {num_iteration} ***")
