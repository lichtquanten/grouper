class Aligner():
    def __init__(self, topics):
        """
        Parameters
        ----------
        topics : list of str
            A list of the topics in the output bundle.
        """
        self._topics = topics
        self._windows = []

    def __iter__(self):
        return self

    def next(self):
        if self._windows and self._is_window_ready(self._windows[0]):
            window = self._windows.pop(0)
            return (window['data'], window['start_time'], window['end_time'])
        else:
            raise StopIteration

    def _add_window(self, start_time, end_time):
        """Appends a blank new window to the buffer"""
        self._windows.append(
            {
                'data': {topic: None for topic in self._topics},
                'start_time': start_time,
                'end_time': end_time,
                'statuses': {topic: False for topic in self._topics},
            })

    @staticmethod
    def _is_window_ready(window):
        return all(window['statuses'].values())

    def put(self, topic, datum, start_time, end_time):
        """
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
        for i in xrange(len(self._windows)):
            if not self._windows[i]['statuses'][topic]:
                self._windows[i]['statuses'][topic] = True
                self._windows[i]['data'][topic] = datum
                return
        self._add_window(start_time, end_time)
        self._windows[-1]['statuses'][topic] = True
        self._windows[-1]['data'][topic] = datum
