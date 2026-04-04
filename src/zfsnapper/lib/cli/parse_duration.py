from dateutil.relativedelta import relativedelta


class ParseError(Exception):
    def __init__(self, input: str, msg: str) -> None:
        super().__init__(f'Failed to parse duration "{input}": {msg}')


# input has format like 2y5m7d3h
def parse_duration(input: str) -> relativedelta:
    res: dict[str, int] = dict()
    start = 0

    for i, c in enumerate(input):
        if c not in {'h', 'd', 'w', 'm', 'y'}:
            continue
        num = input[start:i]
        start = i+1
        if not num:
            raise ParseError(input, f"Unit is without number: {c}")
        if c in res:
            raise ParseError(input, f"Duplicate unit: {c}")
        try:
            res[c] = int(num)
        except ValueError:
            raise ParseError(input, f"Invalid number: {num}")

    if not start == len(input):
        raise ParseError(input, f"Number is without unit: {{input[start:]}}")

    return relativedelta(
        years=res.get('y', 0),
        months=res.get('m', 0),
        weeks=res.get('w', 0),
        days=res.get('d', 0),
        hours=res.get('h', 0)
    )
