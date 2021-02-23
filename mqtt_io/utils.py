from typing import Coroutine, Any, cast


class PriorityCoro:
    def __init__(self, coro: Coroutine[Any, Any, Any], priority: int):
        self.coro = coro
        self.priority = priority

    def __lt__(self, other: Any) -> bool:
        return cast(bool, self.priority < other.priority)

    def __eq__(self, other: Any) -> bool:
        return cast(bool, self.priority == other.priority)
