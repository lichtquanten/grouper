class Combiner():
    """Merges all data from a time window into a dictionary.
    A list of topics are specified on initialization. The data from a time
    window is not sent to the callbacks until there is data for each topic.
    """

    def __init__(self, start_time, window_duration, topics):
        self._last_time = start_time
        self._window_duration = window_duration
        self.topics = topics
        self._windows = []
        self._add_window()


    def __iter__(self):
        return self

    def next(self):
        if self._windows and self._is_window_ready(0):
            window = self._windows.pop(0)
            return window['data'], window['start_time'], window['end_time']
        else:
            raise StopIteration

    def _add_window(self):
        """Appends a blank new window to the buffer, advances the time.
        """
        self._windows.append(
            {
                'data': {topic: None for topic in self.topics},
                'start_time': self._last_time,
                'end_time': self._last_time + self._window_duration,
                'statuses': {topic: False for topic in self.topics},
                'overlaps': {topic: 0 for topic in self.topics},
            })
        self._last_time += self._window_duration

    def _is_window_ready(self, index):
        """Determines if every topic in a window has been marked as handled.
        Args:
            index: The index of a window in the window buffer. This window
                is checked for readiness.
        Returns:
            True if the status of all topics in the window have True status.
        """
        return all(self._windows[index]['statuses'].values())

    @staticmethod
    def _overlap(start_time, end_time, window_start_time, window_end_time):
        """
        Args:
        Returns:
            A number [0, 1.0] that gives the proportion of overlap.
        """
        if start_time > window_end_time or end_time < window_start_time:
            return 0
        start = max(start_time, window_start_time)
        end = min(end_time, window_end_time)
        return (end - start) / float(window_end_time - window_start_time)

    def put(self, topic, data, start_time, end_time):
        """
        Args:
            topic:
            data:
            start_time:
            end_time:
        """
        # Add a window if there are none
        if not self._windows:
            self._add_window()
        # If the data ends before the earliest buffer starts, ignore it
        if end_time < self._windows[0]['start_time']:
            return
        # Make sure that all of the needed windows are there
        while not self._windows[-1]['start_time'] > end_time:
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
                    window['data'][topic] = data
                    window['overlaps'][topic] = overlap
