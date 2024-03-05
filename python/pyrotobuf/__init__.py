from functools import partial
from dataclasses import dataclass, fields
from typing import TypeVar

from . import _pyrotobuf

T = TypeVar("T")


class Messages:
    def __init__(self, pool):
        self._pool = pool

    def __getitem__(self, name):
        return self._pool.get_message_by_name(name)


class Method:
    def __init__(self, fn, input, output):
        self.input = partial(_pyrotobuf.message, input)
        self.output = partial(_pyrotobuf.message, output)
        self._fn = fn

    def __call__(self, data=None, /, **kwargs):
        message = self.Input(data, **kwargs)
        print(self)
        return self._fn(self, message)


def snake_to_pascal(snake_str):
    return "".join(word.capitalize() for word in snake_str.split("_"))


class Descriptors:
    def __init__(self, data: bytes):
        self._pool = _pyrotobuf.DescriptorPool(data)
        self._registered = {}
        self.messages = Messages(self._pool)

        # FIXME: Hack
        class Message:
            _pool = self._pool

            def from_json(self, name, json):
                descriptor = self._pool.get_message_by_name(name)
                return _pyrotobuf.Message.from_json(descriptor, json)

            def __call__(self, name, data=None, /, **kwargs):
                descriptor = self._pool.get_message_by_name(name)
                return _pyrotobuf.Message(descriptor, data, **kwargs)

        self.Message = Message()

    def message(self, name):
        descriptor = self._pool.get_message_by_name(name)

        def inner(cls):
            if name in self._registered:
                raise RuntimeError("Duplicate message")
            cls = dataclass(cls)
            attribute_map = _build_attribute_map(cls, descriptor)
            self._registered[descriptor.full_name] = cls, attribute_map
            return _patch_dataclass(cls, descriptor, self, attribute_map)

        return inner

    def service(self, name):
        descriptor = self._pool.get_service_by_name(name)
        methods = descriptor.methods()

        table = {}

        def __getitem__(self, name, data=None, /, **kwargs):
            attr_name = table[name]
            return getattr(self, attr_name)

        def make_rpc_method(fn, input, output):
            Input = partial(_pyrotobuf.Message, input)
            Output = partial(_pyrotobuf.Message, output)

            def rpc_method(self, data=None, /, **kwargs):
                message = Input(data, **kwargs)
                return fn(self, message)

            rpc_method.Input = Input
            rpc_method.Output = Output
            return rpc_method

        def inner(cls):
            for attr_name, attr_value in cls.__dict__.items():
                if callable(attr_value) and not attr_name.startswith("__"):
                    method_name = snake_to_pascal(attr_name)
                    method = methods[method_name]
                    table[method_name] = attr_name
                    setattr(
                        cls,
                        attr_name,
                        make_rpc_method(attr_value, method.input_message, method.output_message),
                    )

            setattr(cls, "__getitem__", __getitem__)
            return cls

        return inner

    def from_message(self, message):
        field_type = self._registered[message.descriptor.full_name][0]
        return _from_message(field_type, message)

    def to_message(self, obj):
        descriptor, _, _ = obj.__protobuf__

        message = _pyrotobuf.Message(descriptor)
        _fill_message(obj, message)
        return message


def _patch_dataclass(cls, descriptor, protos, attribute_map):
    cls.__protobuf__ = descriptor, protos, attribute_map

    # cls.from_bytes = classmethod(_from_bytes)
    # cls.to_bytes = _to_bytes
    # cls.__bytes__ = _to_bytes
    # cls.__str__ = to_text
    return cls


def _from_message(cls: type[T], message: _pyrotobuf.Message) -> T:
    descriptor, protos, attribute_map = cls.__protobuf__

    attrs = {}
    for field, _, descriptor in attribute_map:
        value = getattr(message, field)
        if isinstance(value, _pyrotobuf.Message):
            field_type = protos._registered[descriptor.full_name][0]
            value = _from_message(field_type, value)
        attrs[field] = value
    return cls(**attrs)


def _fill_message(instance: T, message: _pyrotobuf.Message):
    _, _, attribute_map = instance.__protobuf__

    for field, _, descriptor in attribute_map:
        value = getattr(instance, field)
        if isinstance(descriptor, _pyrotobuf.MessageDescriptor):
            sub_message = _pyrotobuf.Message(descriptor)
            value = _fill_message(value, sub_message)
        setattr(message, field, value)
    return message


def _from_bytes(cls: type[T], msg: bytes) -> T:
    descriptor, _, _ = cls.__protobuf__

    message = _pyrotobuf.Message(descriptor, msg)
    return _from_message(cls, message)


def _to_bytes(self, **kwargs) -> bytes:
    descriptor, _, _ = self.__protobuf__

    message = _pyrotobuf.Message(descriptor)
    _fill_message(self, message)
    return message.to_bytes(**kwargs)


def _build_attribute_map(cls, descriptor):
    dataclass_fields = {field.name: field for field in fields(cls)}
    protobuf_fields = {field.name: field for field in descriptor.fields()}

    # if protobuf is larger than dataclass, warn
    protobuf_fields.keys() - dataclass_fields.keys()
    if protobuf_fields:
        ...

    # if dataclass is larger than protobuf:
    #  - if fields have default value, warn
    #  - crash
    dataclass_only_fields = dataclass_fields.keys() - protobuf_fields.keys()
    if dataclass_only_fields:
        ...

    mappers = []
    for name, protobuf_field in protobuf_fields.items():
        try:
            dataclass_field = dataclass_fields[name]
        except LookupError:
            continue

        mappers.append((name, dataclass_field.type, protobuf_field.kind))

    return mappers
