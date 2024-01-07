import codecs
from importlib.resources import files

from . import Descriptors


file_descriptor_set_path = files("prost_py") / "file_descriptor_set.bin"
with file_descriptor_set_path.open("rb") as fp:
    protos = Descriptors(fp.read())


@protos.message("package.MyMessage")
class MyMessage:
    foo: int
    bar: int


@protos.message("package.Rot13Request")
class Rot13Request:
    text: str


@protos.message("package.Rot13Response")
class Rot13Response:
    transformed_text: str


@protos.service("package.Rot13Service")
class Rot13Service:
    # async def transform(self, request: Rot13Request) -> Rot13Response:
    async def transform(self, request):
        return Rot13Response(transformed_text=codecs.encode(request.text, "rot_13"))


@protos.message("package.NestedMessage.SubMessage")
class SubMessage:
    foo: int


@protos.message("package.NestedMessage")
class NestedMessage:
    sub: SubMessage | None = None
    text: str | None = None


if __name__ == "__main__":
    message = MyMessage.from_bytes(b"\x08\x96\x01")
    print(message)

    message = NestedMessage(sub=SubMessage(foo=10), text="hi")
    output = bytes(message)
    print(output)
    print(NestedMessage.from_bytes(output))

    message = protos.message("package.Rot13Request", text="test")
    print(message)
    print(bytes(message))
