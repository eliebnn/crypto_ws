import datetime as dt
import time


def obj_to_list(value):

    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return list(value.keys())
    else:
        return [value]


class Timer:
    """
    A class used to represent a Timer.

    ...

    Attributes
    ----------
    limit : int
        a limit in seconds that the timer checks against
    _wait : int
        a time in seconds that the timer will wait
    now : datetime
        the current datetime when the Timer object is initialized

    Methods
    ----------
    time_taken:
        Returns the time in seconds since the Timer object was last reset.
    reached_limit:
        Checks if the timer has reached the limit.

    Methods
    -------
    wait():
        Pause execution of the program for a certain number of seconds.
    reset_now():
        Resets the timer's start time to the current time.
    """

    def __init__(self, limit=60, wait=60):
        """
        Constructs all the necessary attributes for the Timer object.

        Parameters
        ----------
            limit : float
                the time limit in seconds (default is 60)
            wait : float
                the wait time in seconds (default is 60)
        """
        self.limit = limit
        self._wait = wait
        self.now = dt.datetime.now()

    @property
    def time_taken(self):
        """
        Returns the time in seconds since the Timer object was last reset.

        Returns
        -------
        float
            The time taken in seconds.
        """
        return (dt.datetime.now() - self.now).total_seconds()

    @property
    def reached_limit(self):
        """
        Checks if the timer has reached the limit.

        Returns
        -------
        bool
            True if the time taken is greater than or equal to the limit, False otherwise.
        """
        return True if self.time_taken >= self.limit else False

    def wait(self):
        """
        Pause execution of the program for the _wait seconds.
        """
        time.sleep(self._wait)

    def reset_now(self):
        """
        Resets the timer's start time to the current time.
        """
        self.now = dt.datetime.now()
