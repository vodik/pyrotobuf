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


def test_message_fields_by_name(protos):
    message = protos.Message("test.v1.TestMessage")
    message.greeting = "Test Greeting"
    assert message.greeting == "Test Greeting"

    with pytest.raises(AttributeError):
        _ = message.unknown

    with pytest.raises(AttributeError):
        message.unknown = "Should fail"


def test_message_fields_by_number(protos):
    message = protos.Message("test.v1.TestMessage")
    message[1] = "Test Greeting"
    assert message[1] == "Test Greeting"

    with pytest.raises(IndexError):
        _ = message[2]

    with pytest.raises(IndexError):
        message[2] = "Should fail"


def test_binary_roundtrip(protos):
    message = protos.Message("test.v1.TestMessage", greeting="Hello World")
    protobuf = bytes(message)
    assert protobuf == b"\n\x0bHello World"
    assert protos.Message("test.v1.TestMessage", protobuf) == message


def test_text_roundtrip(protos):
    message = protos.Message("test.v1.TestMessage", greeting="Hello World")
    protobuf = str(message)
    assert protobuf == 'greeting:"Hello World"'
    assert protos.Message("test.v1.TestMessage", protobuf) == message

    protobuf = message.to_text(pretty=True)
    assert protobuf == 'greeting: "Hello World"'
    assert protos.Message("test.v1.TestMessage", protobuf) == message


def test_dataclass_roundtrip(protos):
    @protos.message("test.v1.TestMessage")
    class TestMessage:
        greeting: str

    dataclass = TestMessage(greeting="Hello World")
    protobuf = bytes(dataclass)
    assert protobuf == b"\n\x0bHello World"
    assert TestMessage.from_bytes(protobuf) == dataclass
