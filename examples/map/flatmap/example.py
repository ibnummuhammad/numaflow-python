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
        datum_str = datum.value.decode("utf-8")
        print("datum_str...")
        print(datum_str)
        pesan_dict = json.loads(datum_str)
        print("pesan_dict...")
        print(pesan_dict)
        messages = Messages()
        print("messages...1")
        print(messages)
        nilai_str = json.dumps(pesan_dict)
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
