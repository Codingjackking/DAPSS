import threading


class LamportClock:
    """
    Lamport logical clock implementation
    Usage:
        clock = LamportClock()

        # On local event (send message, local computation):
        timestamp = clock.tick()

        # On receiving message with timestamp :
        clock.update(received_timestamp)
    """

    def __init__(self, initial_time: int = 0):
        """
        Initialize Lamport clock.

        Args:
            initial_time: Starting value for the clock (default: 0)
        """
        self._time = initial_time
        self._lock = threading.Lock()
    
    # ------------------------------------------------------------------

    def tick(self) -> int:
        """
        Increment the clock for a local event and return the new timestamp

        Returns:
            The new logical timestamp after incrementing
        """
        with self._lock:
            self._time += 1
            return self._time
    # ------------------------------------------------------------------
    def update(self, received_timestamp: int) -> int:
        """
        Update the clock based on a received timestamp.

        new_time = max(local_clock, received_time) + 1

        Args:
            received_timestamp: The Lamport timestamp from the received message

        Returns:
            The new logical timestamp after updating
        """
        with self._lock:
            self._time = max(self._time, received_timestamp) + 1
            return self._time

    # ------------------------------------------------------------------
    def get_time(self) -> int:
        """
        Get the current logical timestamp

        Returns:
            Current logical timestamp
        """
        with self._lock:
            return self._time

    # ------------------------------------------------------------------
    def __str__(self) -> str:
        return f"LamportClock(time={self.get_time()})"

