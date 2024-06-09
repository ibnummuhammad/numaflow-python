from pynumaflow.mapper import Messages, Datum, Mapper


class Flatmap(Mapper):
    """
    This is a class that inherits from the Mapper class.
    It implements the handler method that is called for each datum.
    """

    def handler(self, keys: list[str], datum: Datum) -> Messages:
        import json

        from pynumaflow.mapper import Message

        print("keys...")
        print(keys)
        print("datum...")
        print(datum)
        print("datum.value...")
        print(datum.value)
        print("datum.event_time...")
        print(datum.event_time)
        print("datum.watermark...")
        print(datum.watermark)
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        datum_str = datum.value.decode("utf-8")
        print("datum_str...")
        print(datum_str)
        pesan = val.decode("utf-8")
        print("pesan...")
        print(pesan)
        pesan_dict = json.loads(pesan)
        print("pesan_dict...")
        print(pesan_dict)
        messages = Messages()
        print("messages...1")
        print(messages)
        nilai = """{ "schema": { "type": "struct", "fields": [ { "type": "struct", "fields": [ { "type": "string", "optional": false, "field": "params" }, { "type": "string", "optional": false, "field": "payload" }, { "type": "string", "optional": false, "field": "etl_id" }, { "type": "string", "optional": false, "field": "etl_id_ts" }, { "type": "string", "optional": false, "field": "etl_id_partition" }, { "type": "string", "optional": false, "field": "run_ts" } ], "optional": true, "name": "dbserver1.inventory.customers.Value", "field": "before" }, { "type": "struct", "fields": [ { "type": "string", "optional": false, "field": "params" }, { "type": "string", "optional": false, "field": "payload" }, { "type": "string", "optional": false, "field": "etl_id" }, { "type": "string", "optional": false, "field": "etl_id_ts" }, { "type": "string", "optional": false, "field": "etl_id_partition" }, { "type": "string", "optional": false, "field": "run_ts" } ], "optional": true, "name": "dbserver1.inventory.customers.Value", "field": "after" }, { "type": "struct", "fields": [ { "type": "string", "optional": false, "field": "version" }, { "type": "string", "optional": false, "field": "connector" }, { "type": "string", "optional": false, "field": "name" }, { "type": "int64", "optional": false, "field": "ts_ms" }, { "type": "string", "optional": true, "name": "io.debezium.data.Enum", "version": 1, "parameters": { "allowed": "true,last,false,incremental" }, "default": "false", "field": "snapshot" }, { "type": "string", "optional": false, "field": "db" }, { "type": "string", "optional": true, "field": "sequence" }, { "type": "string", "optional": true, "field": "table" }, { "type": "int64", "optional": false, "field": "server_id" }, { "type": "string", "optional": true, "field": "gtid" }, { "type": "string", "optional": false, "field": "file" }, { "type": "int64", "optional": false, "field": "pos" }, { "type": "int32", "optional": false, "field": "row" }, { "type": "int64", "optional": true, "field": "thread" }, { "type": "string", "optional": true, "field": "query" } ], "optional": false, "name": "io.debezium.connector.mysql.Source", "field": "source" }, { "type": "string", "optional": false, "field": "op" }, { "type": "int64", "optional": true, "field": "ts_ms" }, { "type": "struct", "fields": [ { "type": "string", "optional": false, "field": "id" }, { "type": "int64", "optional": false, "field": "total_order" }, { "type": "int64", "optional": false, "field": "data_collection_order" } ], "optional": true, "name": "event.block", "version": 1, "field": "transaction" } ], "optional": false, "name": "dbserver1.inventory.customers.Envelope", "version": 1 }, "payload": { "before": null, "after": { "params": "{\\"id\\": 1008,\\"first_name\\": \\"bapercobaan\\",\\"last_name\\": \\"bapertama\\",\\"email\\": \\"batesting@email\\"}", "payload": "{\\"id\\": 1008,\\"first_name\\": \\"bapercobaan\\",\\"last_name\\": \\"bapertama\\",\\"email\\": \\"batesting@email\\"}", "etl_id": "muhammad", "etl_id_ts": "ibnu_muhammad@gmail.com", "etl_id_partition": "partisi", "run_ts": "lari" }, "source": { "version": "2.1.4.Final", "connector": "mysql", "name": "dbserver1", "ts_ms": 1717568433000, "snapshot": "false", "db": "inventory", "sequence": null, "table": "customers", "server_id": 1, "gtid": null, "file": "binlog.000002", "pos": 2645, "row": 0, "thread": 23, "query": null }, "op": "c", "ts_ms": 1717568433831, "transaction": null } }"""  # noqa: E501
        nilai_dict = json.loads(nilai)
        print("nilai_dict...1")
        print(nilai_dict)
        nilai_dict["payload"]["after"]["params"] = json.dumps(pesan_dict["payload"]["after"])
        nilai_dict["payload"]["after"]["payload"] = json.dumps(pesan_dict["payload"]["after"])
        print("nilai_dict...2")
        print(nilai_dict)
        nilai_str = json.dumps(nilai_dict)
        print("nilai_str...")
        print(nilai_str)
        messages.append(Message(str.encode(nilai_str)))
        print("messages...2")
        print(messages)
        return messages


if __name__ == "__main__":
    """
    This example shows how to use the Flatmap mapper.
    We use a class as handler, but a function can be used as well.
    """
    from pynumaflow.mapper import MapServer

    grpc_server = MapServer(Flatmap())
    grpc_server.start()
