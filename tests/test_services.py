import codecs

import pytest


@pytest.fixture(scope="session")
def protos(protoc):
    return protoc(
        """
        syntax = "proto3";

        package test.v1;

        message Rot13Request {
            string text = 1;
        }

        message Rot13Response {
            string rot13 = 1;
        }

        service Rot13Service {
            rpc Transform(Rot13Request) returns (Rot13Response) {}
        }
        """
    )


@pytest.mark.anyio
async def test_unary_unary_service(protos):
    @protos.service("test.v1.Rot13Service")
    class Rot13Service:
        async def transform(self, request):
            output = codecs.encode(request.text, "rot_13")
            return self.transform.Output(rot13=output)

    service = Rot13Service()

    response = await service.transform(text="Hello World")
    assert response.rot13 == "Uryyb Jbeyq"
    assert bytes(response) == b"\n\x0bUryyb Jbeyq"

    assert await service["Transform"](text="Hello World") == response
    assert await service.transform(b"\n\x0bHello World") == response
