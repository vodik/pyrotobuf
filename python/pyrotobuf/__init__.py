from dataclasses import dataclass, fields
from functools import partial

from . import _pyrotobuf


class Descriptors:
    def __init__(self, data: bytes):
        self._pool = _pyrotobuf.DescriptorPool(data)
        self._messages: dict[str, _pyrotobuf.Message] = {}

    def patch_dataclass(self, cls, descriptor, attribute_map):
        cls.__protobuf__ = descriptor, attribute_map

        cls.from_bytes = partial(self.from_bytes, cls)
        cls.to_bytes = lambda s: self.to_bytes(s)
        cls.to_text = lambda s: self.to_text(s)

        # cls.__str__ = lambda s: self.to_text(s)
        cls.__bytes__ = lambda s: self.to_bytes(s)
        return cls

    def Message(self, name, data=None, /, **kwargs):
        descriptor = self._pool.get_message_by_name(name)
        return _pyrotobuf.Message(descriptor, data, **kwargs)

    def message(self, name):
        descriptor = self._pool.get_message_by_name(name)

        def inner(cls):
            if name in self._messages:
                raise RuntimeError("Duplicate message")
            cls = dataclass(cls)
            attribute_map = build_attribute_map(cls, descriptor)
            self._messages[descriptor.full_name] = cls, attribute_map
            return self.patch_dataclass(cls, descriptor, attribute_map)

        return inner

    def from_bytes[T](self, cls: type[T], msg: bytes) -> T:
        descriptor, _ = cls.__protobuf__
        message = _pyrotobuf.Message(descriptor, msg)
        return self.from_message(cls, message)

    def from_message[T](self, cls: type[T], message: _pyrotobuf.Message) -> T:
        descriptor, attribute_map = cls.__protobuf__

        attrs = {}
        for field, _, descriptor in attribute_map:
            value = getattr(message, field)
            if isinstance(value, _pyrotobuf.Message):
                field_type = self._messages[descriptor.full_name][0]
                value = self.from_message(field_type, value)
            attrs[field] = value
        return cls(**attrs)

    def to_bytes[T](self, instance: T) -> bytes:
        descriptor, _ = instance.__protobuf__
        message = _pyrotobuf.Message(descriptor)
        self.fill_message(instance, message)
        return bytes(message)

    def to_text[T](self, instance: T) -> str:
        descriptor, _ = instance.__protobuf__
        message = _pyrotobuf.Message(descriptor)
        self.fill_message(instance, message)
        return str(message)

    def fill_message[T](self, instance: T, message: _pyrotobuf.Message):
        _, attribute_map = instance.__protobuf__
        for field, _, descriptor in attribute_map:
            value = getattr(instance, field)
            if isinstance(descriptor, _pyrotobuf.MessageDescriptor):
                sub_message = _pyrotobuf.Message(descriptor)
                value = self.fill_message(value, sub_message)
            setattr(message, field, value)
        return message


def build_attribute_map(cls, descriptor):
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
