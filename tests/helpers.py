from typing import Any, Dict, List, Tuple


def get_item(append: str = None) -> Tuple[str, Dict[str, Any]]:
    key = f"insert-test-key{'-'+append if append is not None else ''}"
    document = {
        "foo": "bar",
        "lorem": "ipsum dolor sit amet",
    }

    return key, document


def get_items(items: int = 10, append: str = None) -> List[Tuple[str, Dict[str, Any]]]:
    ret = []
    for i in range(0, items):
        key = f"insert-multi-test-key{'-'+append if append is not None else ''}-{i}"
        document = {
            "foo": "bar",
            "lorem": "ipsum dolor sit amet",
            "index": i,
        }
        ret.append((key, document))

    return ret
