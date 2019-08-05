"""A whole bunch of generators.

What is the difference between a window and a block?
"""
from abc import ABCMeta, abstractmethod
import collections
import buffer
import numpy as np

class Grouper:
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    def __iter__(self):
        return self

    @abstractmethod
    def put(self, data, start_time, end_time):
        pass

class Counter(Grouper):
    """Counts how many consecutive data satisfy `is_valid`.
    """
    def __init__(self, is_valid):
        """
        Parameters
        ----------
        is_valid : callable
            A function that takes a `datum` as input and returns a bool.
        """
        self._is_valid = is_valid
        self._counter = 0
        self._buffer = []

    def next(self):
        """
        Returns
        -------
        int
            The number of consecutive valid `datum` received prior to the
            earliest received `datum` in the buffer.
        rospy.time.Time
            The start time of the earliest received `datum` in the buffer.
        rospy.time.Time
            The end time of the earliest received `datum` in the buffer.

        Raises
        ------
        StopIteration
            When the buffer is empty.
        """
        if not self._buffer:
            raise StopIteration
        return self._buffer.pop(0)

    def put(self, datum, start_time, end_time):
        self._buffer.append((self._counter, start_time, end_time))
        if self._is_valid(datum):
            self._counter += 1
        else:
            self._counter = 0

class Block(Grouper):
    """Divides data into blocks of fixed length.

    Wrapper to BlockBuffer that tracks start, end time of blocks.
    Accepts timestamped input data. Produces timestamped blocks.
    """
    def __init__(self, block_size=None, block_buffer=None):
        if block_size is None and block_buffer is None:
            raise Exception('Must specify block_size or block_buffer')
        if block_buffer is not None:
            self._block_buffer = block_buffer
        elif block_size is not None:
            self._block_buffer = buffer.BlockBuffer(block_size)
        # Change times to contain dictionaries, not tuples
        self._times = []
        self._last = None

    def __iter__(self):
        for block in self._block_buffer:
            if not self._last:
                self._last = self._times[0][1]
            l = len(block)
            while l > 0:
                # somewhat important >= vs > decision
                # This is absolutely unreadable
                if self._times[0][0] >= l:
                    t = self._len2time(l, *self._times[0])
                    end = self._times[0][1] + t
                    self._times[0][0] -= l
                    self._times[0][1] += t
                    l = 0
                else:
                    l -= self._times[0][0]
                    del self._times[0]

            yield (block, self._last, end)
            self._last = end

    @staticmethod
    def _len2time(segment_length, length, start, end):
        """
        Args:
            segment_length:
            length:
            start:
            end:
        """
        return (end - start) * float(segment_length) / length

    def put(self, data, start_time, end_time):
        """Add data to the buffer."""
        self._block_buffer.put(data)
        self._times.append([len(data), start_time, end_time])

class Window(Grouper):
    """Divides data into windows at fixed offsets from initial start time.

    Windows are of duration window_duration. First window starts
    at start_time. Any data from earlier is ignored. Assumes that data is
    received in temporal order.
    """
    def __init__(self, start_time, window_duration):
        self._start_time = start_time
        self._window_duration = window_duration
        self._buffer = []
        self._current = {
            'start_time': start_time,
            'end_time': start_time + window_duration,
            'data': []
        }

    def __iter__(self):
        return self

    def next(self):
        if not self._buffer:
            raise StopIteration
        window = self._buffer.pop(0)
        return (window['data'], window['start_time'], window['end_time'])

    def _next_window(self):
        """Advances the to the following window. Adds the current window to the
        buffer.
        """
        new_window = {
            'start_time': self._current['end_time'],
            'end_time': self._current['end_time'] + self._window_duration,
            'data': []
        }
        self._buffer.append(self._current)
        self._current = new_window

    def put(self, data, start_time, end_time):
        # Ignore data ending before current window starts
        if end_time < self._start_time:
            return
        # Ship window if it ends before the data ends
        while start_time > self._current['end_time']:
            self._next_window()
        # Add to and ship all windows that it ends after
        while end_time > self._current['end_time']:
            self._current['data'].append(data)
            self._next_window()
        # Add to the window that it ends in
        self._current['data'].append(data)
class History(Grouper):
    """Reports the last `length` entries.
    """
    def __init__(self, length):
        """
        Args:
            length (int):
        """
        self.length = length
        self.buffer = []
        self.times = []

    def __iter__(self):
        return self

    def next(self):
        if len(self.buffer) <= self.length:
            raise StopIteration
        out = self.buffer[:self.length], self.times[self.length][0], self.times[self.length][1]
        del self.buffer[0]
        del self.times[0]
        return out

    def put(self, data, start_time, end_time):
        self.buffer.append(data)
        self.times.append((start_time, end_time))

class Neighborhood(Grouper):
    def __init__(self, is_valid, length):
        self.is_valid = is_valid
        self.length = length
        self.buffer = []

    def __iter__(self):
        while len(self.buffer) > self.length:
          # while not self.buffer[-1]['handled']:
          if self.is_valid([x['data'] for x in self.buffer[:self.length]]):
              for x in self.buffer:
                  if not x['handled']:
                      yield True, x['start_time'], x['end_time']
                      x['handled'] = True
          else:
              if not self.buffer[0]['handled']:
                  yield False, self.buffer[0]['start_time'], self.buffer[0]['end_time']
          del self.buffer[0]

    def put(self, data, start_time, end_time):
        self.buffer.append({
            'data': data,
            'start_time': start_time,
            'end_time': end_time,
            'handled': False
        })
