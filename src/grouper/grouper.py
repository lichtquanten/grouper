"""A module for grouping timestamped data."""

from abc import ABCMeta, abstractmethod

class Grouper(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    def __iter__(self):
        return self

    @abstractmethod
    def next(self):
        pass

    @abstractmethod
    def put(self, data, start_time, end_time):
        pass


class Block(Grouper):
    """Divides data into fixed length blocks.

    A block consists of `length` consecutive datum. Every input datum
    appears in exactly 1 block. The start and end time of a block is determined
    by the start time of the first datum in the block and the end time of the
    last datum in the block, respectively.

    Data are assumed to be `put` in temporal order.
    """
    def __init__(self, block_size):
        self._block_size = self._block_size
        self._output_buffer = []
        self._reset_data()

    def _reset_data():
        self._data = []
        self._start_time = None
        self._end_time = None

    def next(self):
        """
        Returns
        -------
        list
            A list of consecutive data, of length `length`.
        any
            The start time of the first datum in the block.
        any
            The end time of the last datum in the block.

        Raises
        ------
        StopIteration
            When there are no more available blocks.
        """
        if not self._output_buffer:
            raise StopIteration
        return self._output_buffer.pop(0)

    def put(self, datum, start_time, end_time):
        """Add `datum` to the buffer, create a block if possible.

        `datum` assumed to be after, by timestamp, all previously put data.

        Parameters
        ----------
        datum: any
            Anything.
        start_time : any
            The start time associated with `datum`.
        end_time : any
            The end time associated with `datum`.
        """
        self._data.append(datum)
        if self._start_time is None:
            self._start_time = start_time
        self._end_time = end_time

        if len(self._data) == self._block_size:
            self._output_buffer.append(
                (self._data, self._start_time, self._end_time))
            self._reset_data()


class BlockArrLike(Grouper):
    """Divides data in an array-like object into fixed length blocks.

    To use with Python lists, give [] as the `buffer` argument and operator.add
    as the `concatenate` argument of the constructor. Pass in lists for the
    `data` argument of `put`.

    To use with numpy arrays, give np.array([]) as the `buffer` argument
    and `np.append` as the `concatenate` argument of the constructor. Pass in
    numpy arrays for the `data` argument of `put`.
    """
    def __init__(self, block_size, buffer, concatenate):
        """
        Parameters
        ----------
        block_size : int
            The length of each block.
        buffer : array-like
            A buffer to which data input via `put` will be added using `concatenate`.
            The buffer must be empty.
        concatenate : callable
            Accepts two parameters, `buffer` and `data`. Returns a new buffer
            containing both `buffer` and `data`. `uffer` is of the type given
            in the `buffer` parameter. `data` is of the type given in calls to
            `put`.
        """
        self._block_size = self._block_size
        self._buffer = buffer
        self._concatenate = concatenate

        self._times = []
        self._output_buffer = []
        self._prev_end_time = None

    def next(self):
        if not self._output_buffer:
            raise StopIteration
        return self._output_buffer.pop(0)

    def _get_start_time(self):
        return self._times[0]['start_time']

    def _get_end_time(self, block_length):
        # Skip until the time window in which the block ends
        while block_length > self._times[0]['length']:
            block_length -= self._times[0]['length']
            del self._times[0]

        diff = self._times[0]['end_time'] - self._times[0]['start_time']
        proportion = float(block_length) / self._times[0]['length']
        self._times[0]['start_time'] += diff * proportion
        self._times[0]['length'] -= block_length
        return self._times[0]['start_time']

    def put(self, data, start_time, end_time):
        """
        Parameters
        ----------
        data : array-like
            An array-like containing individual datum.
        start_time : implements __add__, __sub__, __mul__, __div__
            The start time associated with `data`.
        end_time : implements __add__, __sub__, __mul__, __div__
            The end time associatd with `data`.
        """
        self._buffer = self._concatenate(self._buffer, data)
        self._times.append({
            'length': len(data),
            'start_time': start_time,
            'end_time': end_time
        })
        while len(self._buffer) >= self._block_size:
            block = self._buffer[:self._block_size]
            blk_start_time = self._get_start_time()
            blk_end_time = self._get_end_time(len(block))
            self._output_buffer.append((block, blk_start_time, blk_end_time))
            self._buffer = self._buffer[self._block_size:]


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
        any
            The start time of the earliest received `datum` in the buffer.
        any
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
        """Evalute the validity of `datum` and update the counter.

        `datum` assumed to be after, by timestamp, all previously put data.

        Parameters
        ----------
        datum: any
            Anything.
        start_time : any
            The start time associated with `datum`.
        end_time : any
            The end time associated with `datum`.
        """
        self._buffer.append((self._counter, start_time, end_time))
        if self._is_valid(datum):
            self._counter += 1
        else:
            self._counter = 0


class History(Grouper):
    """Provides a list of the last `length` data input before each datum.

    Consider a history with length 100. After inputting 101 data, a list of
    100 data would be produced, with its timestamp given by the timestamp of
    the 101st datum.
    """
    def __init__(self, length):
        """
        Parameters
        ----------
        length : int
            The number of data to be included in each history.
        """
        self._length = length
        self._data = []
        self._times = []

    def next(self):
        """
        Returns
        -------
        list
            The earliest `length` list of data that have not yet been returned.
        any
            The start time of the first received datum after the last datum
            in the list.
        any
            The end time of the first received datum after the last datum in
            the list.

        Raises
        ------
        StopIteration
            When there are not enough data in the buffer to create a history of
            length `length`.
        """
        if len(self._data) <= self._length:
            raise StopIteration
        start, end = self._times[self._length]
        out = self._data[:self._length]
        del self._data[0]
        del self._times[0]
        return out, start, end

    def put(self, datum, start_time, end_time):
        """Add `datum` to the buffer.

        `datum` assumed to be after, by timestamp, all previously put data.

        Parameters
        ----------
        datum: any
            Anything.
        start_time : any
            The start time associated with `datum`.
        end_time : any
            The end time associated with `datum`.
        """
        self._data.append(datum)
        self._times.append((start_time, end_time))


class Neighborhood(Grouper):
    """Determine is there is a valid group of contiguous data around some datum.

    Each datum is considered party to neighborhoods of data. A neighborhood
    of data is some successively input (via the `put` method) data. Each datum,
    except for the first `length` - 1 data, are party to `length` neighborhoods
    of length `length`.

    This class determines if any of the neighborhoods of some fixed length
    to which a datum is party is valid. To determine validity, each neighborhood
    is passed to the provided `is_valid` function. Note that validity does not
    suggest properly formatted but rather something more abstract.

    If a datum is found to be an a valid neighborhood, this information is made
    immediately available. The class will not wait to evaluate every neighborhood
    to which the datum is party.
    """
    def __init__(self, is_valid, length):
        """
        Parameters
        ---------
        is_valid : callable
            A function that accepts a list of data and returns a bool.
        length : int
            The length to use when forming neighborhoods.
        """
        self._is_valid = is_valid
        self._length = length
        self._buffer = []
        self._output_buffer = []

    def next(self):
        """
        Returns
        -------
        bool
            True if the datum with the following start, end times is in a valid
            neighborhood, False otherwise.
        any
            The start time of the datum.
        any
            The end time of the datum.

        Raises
        ------
        StopIteration
            When there are no more processed datum.
        """
        if not self._output_buffer:
            raise StopIteration
        return self._output_buffer.pop(0)

    def put(self, datum, start_time, end_time):
        """Add `datum` to the buffer. Evalute the neighborhoods it is party to.

        Parameters
        ----------
        datum: any
            Anything.
        start_time : any
            The start time associated with `datum`.
        end_time : any
            The end time associated with `datum`.
        """
        self._buffer.append({
            'datum': datum,
            'start_time': start_time,
            'end_time': end_time,
            'handled': False
        })
        # Check if the buffer contains a full neighborhood
        if len(self._buffer) == self.length:
            nbhd = [x['data'] for x in self._buffer]
            if self._is_valid(nbhd):
                # Handle all unhandled data in valid neighborhood
                for x in self._buffer:
                    if not x['handled']:
                        self._output_buffer.append(
                            (True, x['start_time'], x['end_time']))
                        x['handled'] = True
            # Remove the first datum from the buffer
            # All of its neighborhoods have been evaluated
            first = self._buffer.pop(0)
            # Output as invalid if not yet handled
            if not first['handled']:
                self._output_buffer.append(
                    (False, first['start_time'], first['end_time']))


class Window(Grouper):
    """Divides timestamped data into windows of time of fixed duration.

    All windows are offset from the initial start time by a multiple of the
    window duration. The first window yielded is the earliest window into which
    the first received datum falls.

    Assumes that data are received in temporal order.
    received in temporal order.
    """
    def __init__(self, start_time, window_duration):
        """
        Parameters
        ----------
        start_time : implements __add__, __sub__
            The time from which the start time of all windows will be
            offset by a multiple of `window_duration`.
        window_duration : implements __add__, __sub__, __div__
            The duration of each window.
        """
        self._start_time = start_time
        self._window_duration = window_duration
        self._windows = []
        self._current = None

    def next(self):
        """
        Returns
        -------
        list
            A list of data in a window. A datum is considered in a window if
            it overlaps in time with the window.
        any
            The start time of the window.
        any
            The end time of the window.

        Raises
        ------
        StopIteration
            When there are no more available windows.
        """
        if not self._windows:
            raise StopIteration
        window = self._windows.pop(0)
        return (window['data'], window['start_time'], window['end_time'])

    def _initialize_current(self):
        """Find appropriate start time for first window, create first window."""
        # Accelerate the start time, if needed
        if start_time > self._start_time:
            diff = start_time - self._start_time
            self._start_time += self._window_duration * int(diff / self._window_duration)

        # Initialize current
        self._current = {
            'start_time': self._start_time,
            'end_time': self._start_time + self._window_duration,
            'data': []
        }

    def _send_current(self):
        """Adds the current window to the windows buffer. Creates a new current window.
        """
        new_window = {
            'start_time': self._current['end_time'],
            'end_time': self._current['end_time'] + self._window_duration,
            'data': []
        }
        self._windows.append(self._current)
        self._current = new_window

    def put(self, datum, start_time, end_time):
        """Evalute the validity of `datum` and update the counter.

        `datum` assumed to be after, by timestamp, all previously put data.

        Parameters
        ----------
        datum: any
            Anything.
        start_time : any
            The start time associated with `datum`.
        end_time : any
            The end time associated with `datum`.
        """
        # Ignore data ending before current window starts
        if end_time < self._start_time:
            return

        # True on first pass
        if not self._current:
            self._initialize_current()

        # Ship window if it ends before the `datum` ends
        while start_time > self._current['end_time']:
            self._send_current()

        # Add `datum` to and ship all windows that end before `datum` ends
        while end_time > self._current['end_time']:
            self._current['data'].append(datum)
            self._send_current()

        # Add `datum` to the window that in which its end time falls
        self._current['data'].append(datum)
