import json


# data_json = """{ "schema": { "type": "struct", "fields": [ { "type": "struct", "fields": [ { "type": "string", "optional": false, "field": "params" }, { "type": "string", "optional": false, "field": "payload" }, { "type": "string", "optional": false, "field": "etl_id" }, { "type": "string", "optional": false, "field": "etl_id_ts" }, { "type": "string", "optional": false, "field": "etl_id_partition" }, { "type": "string", "optional": false, "field": "run_ts" } ], "optional": true, "name": "dbserver1.inventory.customers.Value", "field": "before" }, { "type": "struct", "fields": [ { "type": "string", "optional": false, "field": "params" }, { "type": "string", "optional": false, "field": "payload" }, { "type": "string", "optional": false, "field": "etl_id" }, { "type": "string", "optional": false, "field": "etl_id_ts" }, { "type": "string", "optional": false, "field": "etl_id_partition" }, { "type": "string", "optional": false, "field": "run_ts" } ], "optional": true, "name": "dbserver1.inventory.customers.Value", "field": "after" }, { "type": "struct", "fields": [ { "type": "string", "optional": false, "field": "version" }, { "type": "string", "optional": false, "field": "connector" }, { "type": "string", "optional": false, "field": "name" }, { "type": "int64", "optional": false, "field": "ts_ms" }, { "type": "string", "optional": true, "name": "io.debezium.data.Enum", "version": 1, "parameters": { "allowed": "true,last,false,incremental" }, "default": "false", "field": "snapshot" }, { "type": "string", "optional": false, "field": "db" }, { "type": "string", "optional": true, "field": "sequence" }, { "type": "string", "optional": true, "field": "table" }, { "type": "int64", "optional": false, "field": "server_id" }, { "type": "string", "optional": true, "field": "gtid" }, { "type": "string", "optional": false, "field": "file" }, { "type": "int64", "optional": false, "field": "pos" }, { "type": "int32", "optional": false, "field": "row" }, { "type": "int64", "optional": true, "field": "thread" }, { "type": "string", "optional": true, "field": "query" } ], "optional": false, "name": "io.debezium.connector.mysql.Source", "field": "source" }, { "type": "string", "optional": false, "field": "op" }, { "type": "int64", "optional": true, "field": "ts_ms" }, { "type": "struct", "fields": [ { "type": "string", "optional": false, "field": "id" }, { "type": "int64", "optional": false, "field": "total_order" }, { "type": "int64", "optional": false, "field": "data_collection_order" } ], "optional": true, "name": "event.block", "version": 1, "field": "transaction" } ], "optional": false, "name": "dbserver1.inventory.customers.Envelope", "version": 1 }, "payload": { "before": null, "after": { "params": "{\\"id\\": 1008,\\"first_name\\": \\"bapercobaan\\",\\"last_name\\": \\"bapertama\\",\\"email\\": \\"batesting@email\\"}", "payload": "{\\"id\\": 1008,\\"first_name\\": \\"bapercobaan\\",\\"last_name\\": \\"bapertama\\",\\"email\\": \\"batesting@email\\"}", "etl_id": "muhammad", "etl_id_ts": "ibnu_muhammad@gmail.com", "etl_id_partition": "partisi", "run_ts": "lari" }, "source": { "version": "2.1.4.Final", "connector": "mysql", "name": "dbserver1", "ts_ms": 1717568433000, "snapshot": "false", "db": "inventory", "sequence": null, "table": "customers", "server_id": 1, "gtid": null, "file": "binlog.000002", "pos": 2645, "row": 0, "thread": 23, "query": null }, "op": "c", "ts_ms": 1717568433831, "transaction": null } }"""
# data_dict = json.loads(data_json)
# print(data_dict)
# data_json1 = json.dumps(data_dict)
# print(data_json1)
# data_dict1 = json.loads(data_json1)
# print(data_dict1)
# print(data_dict1["payload"]["after"]["params"])
# data_dict2 = json.loads(data_dict1["payload"]["after"]["params"])
# print(data_dict2)
# print(data_dict1["payload"]["after"]["payload"])
# data_dict2 = json.loads(data_dict1["payload"]["after"]["payload"])
# print(data_dict2)

data_dict = {
    "schema": {
        "type": "struct",
        "fields": [
            {
                "type": "struct",
                "fields": [
                    {"type": "int32", "optional": False, "field": "id"},
                    {"type": "string", "optional": False, "field": "first_name"},
                    {"type": "string", "optional": False, "field": "last_name"},
                    {"type": "string", "optional": False, "field": "email"},
                ],
                "optional": True,
                "name": "dbserver1.inventory.customers.Value",
                "field": "before",
            },
            {
                "type": "struct",
                "fields": [
                    {"type": "int32", "optional": False, "field": "id"},
                    {"type": "string", "optional": False, "field": "first_name"},
                    {"type": "string", "optional": False, "field": "last_name"},
                    {"type": "string", "optional": False, "field": "email"},
                ],
                "optional": True,
                "name": "dbserver1.inventory.customers.Value",
                "field": "after",
            },
            {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": False, "field": "version"},
                    {"type": "string", "optional": False, "field": "connector"},
                    {"type": "string", "optional": False, "field": "name"},
                    {"type": "int64", "optional": False, "field": "ts_ms"},
                    {
                        "type": "string",
                        "optional": True,
                        "name": "io.debezium.data.Enum",
                        "version": 1,
                        "parameters": {"allowed": "true,last,false,incremental"},
                        "default": "false",
                        "field": "snapshot",
                    },
                    {"type": "string", "optional": False, "field": "db"},
                    {"type": "string", "optional": True, "field": "sequence"},
                    {"type": "int64", "optional": True, "field": "ts_us"},
                    {"type": "int64", "optional": True, "field": "ts_ns"},
                    {"type": "string", "optional": True, "field": "table"},
                    {"type": "int64", "optional": False, "field": "server_id"},
                    {"type": "string", "optional": True, "field": "gtid"},
                    {"type": "string", "optional": False, "field": "file"},
                    {"type": "int64", "optional": False, "field": "pos"},
                    {"type": "int32", "optional": False, "field": "row"},
                    {"type": "int64", "optional": True, "field": "thread"},
                    {"type": "string", "optional": True, "field": "query"},
                ],
                "optional": False,
                "name": "io.debezium.connector.mysql.Source",
                "field": "source",
            },
            {"type": "string", "optional": False, "field": "op"},
            {"type": "int64", "optional": True, "field": "ts_ms"},
            {"type": "int64", "optional": True, "field": "ts_us"},
            {"type": "int64", "optional": True, "field": "ts_ns"},
            {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": False, "field": "id"},
                    {"type": "int64", "optional": False, "field": "total_order"},
                    {"type": "int64", "optional": False, "field": "data_collection_order"},
                ],
                "optional": True,
                "name": "event.block",
                "version": 1,
                "field": "transaction",
            },
        ],
        "optional": False,
        "name": "dbserver1.inventory.customers.Envelope",
        "version": 2,
    },
    "payload": {
        "before": None,
        "after": {"id": 1016, "first_name": "xzn", "last_name": "xza", "email": "xzg@email"},
        "source": {
            "version": "2.6.2.Final",
            "connector": "mysql",
            "name": "dbserver1",
            "ts_ms": 1717921632000,
            "snapshot": "false",
            "db": "inventory",
            "sequence": None,
            "ts_us": 1717921632000000,
            "ts_ns": 1717921632000000000,
            "table": "customers",
            "server_id": 1,
            "gtid": None,
            "file": "binlog.000002",
            "pos": 6498,
            "row": 0,
            "thread": 36,
            "query": None,
        },
        "op": "c",
        "ts_ms": 1717921632677,
        "ts_us": 1717921632677862,
        "ts_ns": 1717921632677862000,
        "transaction": None,
    },
}

# data_after = data_dict["payload"]["after"]
# print(json.dumps(data_after))

from datetime import datetime

event_time = "2024-06-09 08:27:12.863000"
event_time_datetime = datetime.strptime(event_time, '%Y-%m-%d %H:%M:%S.%f')
print(type(event_time_datetime))
print(event_time_datetime)

event_time = "2024-06-09 08:27:12.863000"
event_time_datetime = datetime.strptime(event_time, '%Y-%m-%d %H:%M:%S.%f')
print(type(event_time_datetime))
print(event_time_datetime)
print(int(event_time_datetime.timestamp()))