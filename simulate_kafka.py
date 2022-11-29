


from kafka import KafkaProducer
bootstrap_servers = ['172.18.0.4:9092']
topicName = 'cdc_test_topics'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
# Asynchronous by default
future = producer.send(topicName, r"""
    {"schema":{
    "type":"struct",
    "fields":[
        {
            "type":"struct",
            "fields":[
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"int32",
                            "optional":false,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":false,
                    "name":"id",
                    "field":"id"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"int32",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"pin",
                    "field":"pin"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"string",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"status",
                    "field":"status"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"int64",
                            "optional":true,
                            "name":"io.debezium.time.Timestamp",
                            "version":1,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"created",
                    "field":"created"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"string",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"creator_type",
                    "field":"creator_type"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"string",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"creator",
                    "field":"creator"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"int64",
                            "optional":true,
                            "name":"io.debezium.time.Timestamp",
                            "version":1,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"updated",
                    "field":"updated"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"string",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"updator_type",
                    "field":"updator_type"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"string",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"updator",
                    "field":"updator"
                }
            ],
            "optional":true,
            "name":"dbserver1.public.customer.Value",
            "field":"before"
        },
        {
            "type":"struct",
            "fields":[
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"int32",
                            "optional":false,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":false,
                    "name":"id",
                    "field":"id"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"int32",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"pin",
                    "field":"pin"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"string",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"status",
                    "field":"status"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"int64",
                            "optional":true,
                            "name":"io.debezium.time.Timestamp",
                            "version":1,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"created",
                    "field":"created"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"string",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"creator_type",
                    "field":"creator_type"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"string",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"creator",
                    "field":"creator"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"int64",
                            "optional":true,
                            "name":"io.debezium.time.Timestamp",
                            "version":1,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"updated",
                    "field":"updated"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"string",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"updator_type",
                    "field":"updator_type"
                },
                {
                    "type":"struct",
                    "fields":[
                        {
                            "type":"string",
                            "optional":true,
                            "field":"value"
                        },
                        {
                            "type":"boolean",
                            "optional":false,
                            "field":"set"
                        }
                    ],
                    "optional":true,
                    "name":"updator",
                    "field":"updator"
                }
            ],
            "optional":true,
            "name":"dbserver1.public.customer.Value",
            "field":"after"
        },
        {
            "type":"struct",
            "fields":[
                {
                    "type":"string",
                    "optional":false,
                    "field":"version"
                },
                {
                    "type":"string",
                    "optional":false,
                    "field":"connector"
                },
                {
                    "type":"string",
                    "optional":false,
                    "field":"name"
                },
                {
                    "type":"int64",
                    "optional":false,
                    "field":"ts_ms"
                },
                {
                    "type":"string",
                    "optional":true,
                    "name":"io.debezium.data.Enum",
                    "version":1,
                    "parameters":{
                        "allowed":"true,last,false"
                    },
                    "default":"false",
                    "field":"snapshot"
                },
                {
                    "type":"string",
                    "optional":false,
                    "field":"db"
                },
                {
                    "type":"string",
                    "optional":true,
                    "field":"sequence"
                },
                {
                    "type":"string",
                    "optional":false,
                    "field":"schema"
                },
                {
                    "type":"string",
                    "optional":false,
                    "field":"table"
                },
                {
                    "type":"string",
                    "optional":true,
                    "field":"txId"
                },
                {
                    "type":"string",
                    "optional":true,
                    "field":"lsn"
                },
                {
                    "type":"int64",
                    "optional":true,
                    "field":"xmin"
                }
            ],
            "optional":false,
            "name":"io.debezium.connector.postgresql.Source",
            "field":"source"
        },
        {
            "type":"string",
            "optional":false,
            "field":"op"
        },
        {
            "type":"int64",
            "optional":true,
            "field":"ts_ms"
        },
        {
            "type":"struct",
            "fields":[
                {
                    "type":"string",
                    "optional":false,
                    "field":"id"
                },
                {
                    "type":"int64",
                    "optional":false,
                    "field":"total_order"
                },
                {
                    "type":"int64",
                    "optional":false,
                    "field":"data_collection_order"
                }
            ],
            "optional":true,
            "field":"transaction"
        }
    ],
    "optional":false,
    "name":"dbserver1.public.customer.Envelope"
},
"payload":{
    "before":null,
    "after":{
        "id":{
            "value":10000,
            "set":true
        },
        "pin":{
            "value":"123",
            "set":true
        },
        "status":{
            "value":1,
            "set":true
        },
        "created":{
            "value":21312,
            "set":true
        },
        "creator_type":{
            "value":1,
            "set":true
        },
        "creator":{
            "value":"Khan",
            "set":true
        },
        "status_metadata":{
            "value":1,
            "set":true
        },
        "updated":{
            "value":1,
            "set":true
        },
        "updater_type":{
            "value":1,
            "set":true
        },
        "updater":{
            "value":1,
            "set":true
        }
    },
    "source":{
        "version":"1.7.0.13-BETA",
        "connector":"yugabytedb",
        "name":"dbserver1",
        "ts_ms":1416329,
        "snapshot":"false",
        "db":"testdatabase",
        "sequence":"[\"0:0::0:0\",\"3:6::0:0\"]",
        "schema":"public",
        "table":"custsdaomer",
        "txId":"5",
        "lsn":"3:6::0:0",
        "xmin":null
    },
    "op":"u",
    "ts_ms":1667832425466,
    "transaction":null
}
}""".encode('utf-8')).get(timeout=10)

producer.send("cdc_test_topics",r"""
{
	"schema": {
		"type": "struct",
		"fields": [{
			"type": "struct",
			"fields": [{
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": false,
					"name": "io.debezium.data.Uuid",
					"version": 1,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": false,
				"name": "id",
				"field": "id"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "status",
				"field": "status"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "status_metadata",
				"field": "status_metadata"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"name": "io.debezium.data.Uuid",
					"version": 1,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "creator",
				"field": "creator"
			}, {
				"type": "struct",
				"fields": [{
					"type": "int64",
					"optional": true,
					"name": "io.debezium.time.Timestamp",
					"version": 1,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "created",
				"field": "created"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "creator_type",
				"field": "creator_type"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"name": "io.debezium.data.Uuid",
					"version": 1,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "updater",
				"field": "updater"
			}, {
				"type": "struct",
				"fields": [{
					"type": "int64",
					"optional": true,
					"name": "io.debezium.time.Timestamp",
					"version": 1,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "updated",
				"field": "updated"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "updater_type",
				"field": "updater_type"
			}],
			"optional": true,
			"name": "dbserver1.public.customer.Value",
			"field": "before"
		}, {
			"type": "struct",
			"fields": [{
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": false,
					"name": "io.debezium.data.Uuid",
					"version": 1,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": false,
				"name": "id",
				"field": "id"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "status",
				"field": "status"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "status_metadata",
				"field": "status_metadata"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"name": "io.debezium.data.Uuid",
					"version": 1,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "creator",
				"field": "creator"
			}, {
				"type": "struct",
				"fields": [{
					"type": "int64",
					"optional": true,
					"name": "io.debezium.time.Timestamp",
					"version": 1,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "created",
				"field": "created"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "creator_type",
				"field": "creator_type"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"name": "io.debezium.data.Uuid",
					"version": 1,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "updater",
				"field": "updater"
			}, {
				"type": "struct",
				"fields": [{
					"type": "int64",
					"optional": true,
					"name": "io.debezium.time.Timestamp",
					"version": 1,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "updated",
				"field": "updated"
			}, {
				"type": "struct",
				"fields": [{
					"type": "string",
					"optional": true,
					"field": "value"
				}, {
					"type": "boolean",
					"optional": false,
					"field": "set"
				}],
				"optional": true,
				"name": "updater_type",
				"field": "updater_type"
			}],
			"optional": true,
			"name": "dbserver1.public.customer.Value",
			"field": "after"
		}, {
			"type": "struct",
			"fields": [{
				"type": "string",
				"optional": false,
				"field": "version"
			}, {
				"type": "string",
				"optional": false,
				"field": "connector"
			}, {
				"type": "string",
				"optional": false,
				"field": "name"
			}, {
				"type": "int64",
				"optional": false,
				"field": "ts_ms"
			}, {
				"type": "string",
				"optional": true,
				"name": "io.debezium.data.Enum",
				"version": 1,
				"parameters": {
					"allowed": "true,last,false"
				},
				"default": "false",
				"field": "snapshot"
			}, {
				"type": "string",
				"optional": false,
				"field": "db"
			}, {
				"type": "string",
				"optional": true,
				"field": "sequence"
			}, {
				"type": "string",
				"optional": false,
				"field": "schema"
			}, {
				"type": "string",
				"optional": false,
				"field": "table"
			}, {
				"type": "string",
				"optional": true,
				"field": "txId"
			}, {
				"type": "string",
				"optional": true,
				"field": "lsn"
			}, {
				"type": "int64",
				"optional": true,
				"field": "xmin"
			}],
			"optional": false,
			"name": "io.debezium.connector.postgresql.Source",
			"field": "source"
		}, {
			"type": "string",
			"optional": false,
			"field": "op"
		}, {
			"type": "int64",
			"optional": true,
			"field": "ts_ms"
		}, {
			"type": "struct",
			"fields": [{
				"type": "string",
				"optional": false,
				"field": "id"
			}, {
				"type": "int64",
				"optional": false,
				"field": "total_order"
			}, {
				"type": "int64",
				"optional": false,
				"field": "data_collection_order"
			}],
			"optional": true,
			"field": "transaction"
		}],
		"optional": false,
		"name": "dbserver1.public.customer.Envelope"
	},
	"payload": {
		"before": {
			"id": {
				"value": "99923492-6026-45a4-b89c-32d4dde8ccb1",
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
			"ts_ms": -4616124018684,
			"snapshot": "false",
			"db": "yugabyte",
			"sequence": "[null,\"4:36::0:0\"]",
			"schema": "public",
			"table": "customer",
			"txId": "",
			"lsn": "4:36::0:0",
			"xmin": null
		},
		"op": "d",
		"ts_ms": 1669708957938,
		"transaction": null
	}
}""".encode('utf-8')).get(timeout=10)