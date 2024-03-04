import pytest


@pytest.fixture(scope="session")
def protos(protoc):
    return protoc(
        """
        syntax = "proto3";

        package test.v1;

        message TestMessage {
            string greeting = 1;
        }
        """
    )


def test_empty_message(protos):
    message = protos.Message("test.v1.TestMessage")
    assert bytes(message) == b""


def test_binary_roundtrip(protos):
    message = protos.Message("test.v1.TestMessage", greeting="Hello World")
    protobuf = bytes(message)
    assert protobuf == b"\n\x0bHello World"
    assert protos.Message("test.v1.TestMessage", protobuf) == message


def test_json_roundtrip(protos):
    message = protos.Message("test.v1.TestMessage", greeting="Hello World")
    json = message.to_json()
    assert json == '{"greeting":"Hello World"}'
    assert protos.Message.from_json("test.v1.TestMessage", json) == message


def test_text_roundtrip(protos):
    message = protos.Message("test.v1.TestMessage", greeting="Hello World")
    protobuf = str(message)
    assert protobuf == 'greeting:"Hello World"'
    assert protos.Message("test.v1.TestMessage", protobuf) == message

    protobuf = message.to_text(pretty=True)
    assert protobuf == 'greeting: "Hello World"'
    assert protos.Message("test.v1.TestMessage", protobuf) == message


def test_message_fields_by_name(protos):
    message = protos.Message("test.v1.TestMessage")
    message.greeting = "Test Greeting"
    assert message.greeting == "Test Greeting"


def test_message_unknown_fields_by_name(protos):
    message = protos.Message("test.v1.TestMessage")

    with pytest.raises(AttributeError):
        _ = message.unknown

    with pytest.raises(AttributeError):
        message.unknown = "Should fail"


def test_message_fields_by_number(protos):
    message = protos.Message("test.v1.TestMessage")
    message[1] = "Test Greeting"
    assert message[1] == "Test Greeting"


def test_message_unknown_fields_by_number(protos):
    message = protos.Message("test.v1.TestMessage")

    with pytest.raises(IndexError):
        _ = message[2]

    with pytest.raises(IndexError):
        message[2] = "Should fail"


def test_message_iter_fields(protos):
    message = protos.Message("test.v1.TestMessage", greeting="Hello World")

    fields = message.fields()
    assert len(fields) == 1

    descriptor, value = fields[0]
    assert value == "Hello World"
    assert descriptor.number == 1
    assert descriptor.name == "greeting"
    assert descriptor.kind == "string"
    assert descriptor.cadinality == "optional"


def test_dataclass_roundtrip(protos):
    @protos.message("test.v1.TestMessage")
    class TestMessage:
        greeting: str

    dataclass = TestMessage(greeting="Hello World")
    message = protos.to_message(dataclass)
    assert bytes(message) == b"\n\x0bHello World"
    assert str(message) == 'greeting:"Hello World"'

    assert protos.from_message(message) == dataclass
