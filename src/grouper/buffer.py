import numpy as np

class BlockBuffer(object):
    """Divides items from an iterable into blocks.

    Accepts a list (or other iterable) of data. Adds each item from the list
    into a block of size `block_size`. The class is itself a generator and
    yields blocks.
    """

    def __init__(self, block_size):
        self._block_size = block_size
        self._buffer = []
        self.reset()

    def reset(self):
        """Resets the buffer"""
        self._buffer = []

    def put(self, data):
        """Appends data to the buffer"""
        self._buffer.append(data)

    def __iter__(self):
        return self

    def next(self):
        """Extracts the next block in the buffer"""
        if self._block_size > len(self._buffer):
            raise StopIteration
        out = self._buffer[:self._block_size]
        self._buffer = self._buffer[self._block_size:]
        return out

    @property
    def num_blocks(self):
        """Computes the number of complete blocks in the buffer"""
        return int(len(self._buffer) / self._block_size)

    def get_all(self):
        """Returns the buffer.

        Combines the blocks in the buffer and any leftover data into a single
        iterable.
        """
        return self._buffer


class BlockBufferNP(BlockBuffer):
    """A numpy implementation of BlockBuffer"""

    def __init__(self, block_size, dtype):
        """Inits BlockBuffer with block size and numpy data type

        Args:
            block_size: The length of the block, in number of elements.
            dtype: A numpy data type. Data put into the buffer must agree with
                this.
        """
        self._block_size = block_size
        self._dtype = dtype
        self.reset()

    def reset(self):
        """Empties the buffer"""
        self._buffer = np.array([], dtype=self._dtype)

    def put(self, data):
        """Appends an item to the buffer

        Args:
            data: A numpy array.
        """
        self._buffer = np.append(self._buffer, data)
