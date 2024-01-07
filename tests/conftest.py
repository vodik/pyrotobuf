import subprocess
import pyrotobuf
import pytest
from functools import lru_cache


def compile_protobufs(files, includes):
    return pyrotobuf.Descriptors(
        subprocess.check_output(
            [
                "protoc",
                "--include_imports",
                "--include_source_info",
                "-o/dev/stdout",
                *(f"-I{include}" for include in includes),
                *(str(file) for file in files),
            ]
        )
    )


@pytest.fixture(scope="session")
def protoc(tmpdir_factory):
    root = tmpdir_factory.mktemp("protos")

    @lru_cache(maxsize=1024)
    def inner(protos):
        protos_file = root / "protos.protos"
        with protos_file.open("w") as fp:
            fp.write(protos)

        return compile_protobufs([protos_file], [root])

    return inner
