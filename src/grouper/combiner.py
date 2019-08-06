class Combiner():
    """Wait for a datum from each topic and output a complete bundle.

    The first bundle output is the first bundle for which all topics
    provide a datum. This avoids the problem of arbitrarily many windows
    being output for topics that start at different times.

    Data are assummed to be in chronological order for each topic.
    """
    def __init__(self, start_time, window_duration, topics):
        """
        Parameters
        ----------
        start_time : implements __add__
            The time from which the start time of all bundles will be offset
            by a multiple of `window_duration`.
        window_duration : implements __add__
            The duration of time to which each bundle corresponds.
        topics : list of str
            A list of the topics in the output bundle.
        """
        self._next_start_time = start_time
        self._window_duration = window_duration
        self._topics = topics

        self._initialized = {topic: False for topic in self._topics}
        self._windows = []

    def __iter__(self):
        return self

    def next(self):
        """
        Returns
        -------
        dict
            A field for every topic containing the datum with start time, end time
            most overlapping with the start, end time of the bundle.
        any
            Start time of the bundle.
        any
            End time of the bundle.
        Raises
        ------
        StopIteration
            When there are no bundles in which all topics are ready.
        """
        if self._is_window_ready(self._windows[0]):
            window = self._windows.pop(0)
            return (window['data'], window['start_time'], window['end_time'])
        else:
            raise StopIteration

    def _add_window(self):
        """Appends a blank new window to the buffer, advances the time."""
        self._windows.append(
            {
                'data': {topic: None for topic in self._topics},
                'start_time': self._next_start_time,
                'end_time': self._next_start_time + self._window_duration,
                'statuses': {topic: False for topic in self._topics},
                'overlaps': {topic: 0 for topic in self._topics},
            })
        self._next_start_time += self._window_duration

    @staticmethod
    def _is_window_ready(window):
        return all(window['statuses'].values())

    @staticmethod
    def _overlap(start, end, w_start, w_end):
        """Computes the proportion of `w_start` to `w_end` that overlaps with
        `start` to `end`."""
        if start > w_end or end < w_start:
            return 0
        start = max(start, w_start)
        end = min(end, w_end)
        window_duration = w_end - w_start
        return (end - start) / window_duration

    def put(self, topic, datum, start_time, end_time):
        """Adds `datum` to whatever bundles its start, end time overlap with,
        overwritting existing data if it has greater overlap.

        Parameters
        ----------
        topic : str
            A topic from the list of topics provided in the constructor.
        datum : any
            Anything.
        start_time : implements __add__
            The start time of `datum`.
        end_time : implements __add__
            The end time of `datum`.
        """
        if not self._initialized[topic]:
            self._initialized[topic] = True
            # Remove windows that end prior to the start of the first datum
            # on this topic
            while self._windows and start_time > self._windows[0]['end_time']:
                del self._windows[0]
            # Accelerate the next start time, if needed
            if start_time > self._next_start_time:
                # This will only happen if self._windows is empty
                diff = start_time - self._next_start_time
                self._next_start_time += self._window_duration * int(diff / self._window_duration)

        # Add a window if there are none
        if not self._windows:
            self._add_window()
        # If the data ends before the earliest window starts, ignore it
        if end_time < self._windows[0]['start_time']:
            return
        # Make sure that all of the needed windows are there
        while self._windows[-1]['start_time'] <= end_time:
            self._add_window()
        for window in self._windows:
            # Mark earlier windows as ready, for this topic
            if window['end_time'] <= start_time:
                window['statuses'][topic] = True
            # Break if the windows start after the data ends
            elif window['start_time'] > end_time:
                break
            # Edit the overlap if there is overlap
            else:
                overlap = self._overlap(
                    start_time, end_time,
                    window['start_time'], window['end_time'])
                if overlap > window['overlaps'][topic]:
                    window['data'][topic] = datum
                    window['overlaps'][topic] = overlap
