from collections.abc import Collection

from ..transport import CommandRunner
from .model import Property
from .common import _is_empty, _normalize_str


class RawZpool:
    def __init__(self, runner: CommandRunner):
        self._runner = runner

    def get(
        self,
        properties: str | Collection[str],
        targets: str | Collection[str] | None = None,
    ) -> list[Property]:
        properties = _normalize_str(properties)
        targets = _normalize_str(targets)
        if _is_empty(properties):
            raise ValueError("properties must not be empty")
        if _is_empty(targets):
            return []

        cmd: list[str] = [
            'zpool', 'get', '-Hp',
            '-o', 'name,property,value,source',
        ]
        cmd += [','.join(properties)]
        if targets:
            cmd += list(targets)

        lines = self._runner.run_text_command(cmd).splitlines()
        props: list[Property] = []
        for line in lines:
            name, property, value, source = line.split('\t')
            props.append(Property.from_raw(name, property, value, source))
        return props
