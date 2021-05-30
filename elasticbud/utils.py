from __future__ import annotations

from copy import copy

from .errors import AsteriskNotAtListError, InvalidSplatError, RecursedToKeyError


def recurse_splat_key(
    data: Dict[str, Any], value_keys: List[str]
) -> Generator[Any, None, None]:
    value_keys = copy(value_keys)
    """
        recurse to key with splat syntax for specifying "each key in list of objects"
    """
    # try:
    #    if value_keys[-1] == "*":
    #        raise InvalidSplatError(f"cannot end splat with '*': {value_keys}")
    # except IndexError as e:
    #    raise IndexError(f"recursed to empty keys: {data}")

    value_key = value_keys[0]

    if value_key == "*":
        if not isinstance(data, list):
            raise AsteriskNotAtListError(f"data is not a list, has keys {data.keys()}")
        if len(value_keys) == 1:  # last element is *, return data here
            yield from data
        else:  # need to keep recursing beyond wildcard
            for datum in data:
                yield from recurse_splat_key(datum, value_keys[1:])

    else:
        if value_key not in data:
            raise RecursedToKeyError(f"{data} contains no key {value_key}")

        data = data[value_key]

        value_keys.pop(0)
        if len(value_keys) == 0:
            yield data
        else:
            yield from recurse_splat_key(data, value_keys)


def batch(iterable: Iterable, n: int = 1) -> Generator[Iterable, None, None]:
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]
