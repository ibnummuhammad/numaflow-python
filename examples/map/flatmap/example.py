from pynumaflow.mapper import Messages, Message, Datum, MapServer, Mapper

from pynumaflow._constants import _LOGGER


class Flatmap(Mapper):
    """
    This is a class that inherits from the Mapper class.
    It implements the handler method that is called for each datum.
    """

    def handler(self, keys: list[str], datum: Datum) -> Messages:
        _LOGGER.info("type(datum)...")
        _LOGGER.info(type(datum))
        _LOGGER.info("type(datum.value)...")
        _LOGGER.info(type(datum.value))
        _LOGGER.info("type(datum.event_time)...")
        _LOGGER.info(type(datum.event_time))
        _LOGGER.info("type(datum.watermark)...")
        _LOGGER.info(type(datum.watermark))
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        strs = val.decode("utf-8").split(",")
        _LOGGER.info("type(strs)...")
        _LOGGER.info(type(strs))
        messages = Messages()
        _LOGGER.info("type(messages)...")
        _LOGGER.info(type(messages))
        if len(strs) == 0:
            messages.append(Message.to_drop())
            return messages
        for s in strs:
            messages.append(Message(str.encode(s)))
        return messages


if __name__ == "__main__":
    """
    This example shows how to use the Flatmap mapper.
    We use a class as handler, but a function can be used as well.
    """
    grpc_server = MapServer(Flatmap())
    grpc_server.start()
