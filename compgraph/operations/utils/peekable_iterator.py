from typing import Iterator, TypeVar

T = TypeVar('T')


class PeekableIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T]):
        self.iterator = iterator
        self._next_item: T | None = None
        self._cache_next_item()

    def _cache_next_item(self) -> None:
        try:
            self._next_item = next(self.iterator)
        except StopIteration:
            self._next_item = None

    def peek(self) -> T | None:
        return self._next_item

    def __bool__(self) -> bool:
        return self._next_item is not None

    def __iter__(self) -> 'PeekableIterator[T]':
        return self

    def __next__(self) -> T:
        if self._next_item is None:
            raise StopIteration
        to_return = self._next_item
        self._cache_next_item()
        return to_return
