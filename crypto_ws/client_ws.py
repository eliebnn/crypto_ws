import json
import time

import websocket


class WebsocketClient:
    """
    A class to represent a WebSocket client.

    ...

    Attributes
    ----------
    url : str
        a string representing the URL of the WebSocket server
    _socket : websocket.WebSocket
        a WebSocket object representing the connection to the server
    _options :dict
        a dict of options for websocket connection

    Methods
    -------
    connect():
        Establishes a WebSocket connection.
    keep_alive():
        Method to keep the connection alive, to be overridden in subclasses.
    send(msg: dict):
        Sends a message over the WebSocket connection.
    rcv():
        Receives a message over the WebSocket connection.
    run():
        Runs the WebSocket client, attempts to establish a connection and handle exceptions.
    subscribe():
        Placeholder method to be overridden in subclasses, for subscribing to a topic.
    loop():
        Placeholder method to be overridden in subclasses, for handling incoming data.
    """

    def __init__(self, url=None, **options):
        """
        Constructs all the necessary attributes for the WebSocketClient object.

        Parameters
        ----------
            url : str
                the URL of the WebSocket server
        """
        self.url = url
        self._socket = None
        self._options = options

    def _connect(self):
        """
        Establishes a WebSocket connection to the specified URL.
        """
        self._socket = websocket.create_connection(self.url, **self._options)

    def _keep_alive(self):
        """
        Method to keep the WebSocket connection alive.
        To be overridden in subclasses.

        Returns
        -------
        int
            Always returns 0.
        """
        return 0

    def _send(self, msg):
        """
        Sends a JSON message over the WebSocket connection.

        Parameters
        ----------
        msg : dict
            The message to be sent. This should be a dictionary that can be serialized to JSON.
        """
        self._socket.send(json.dumps(msg))

    def _rcv(self):
        """
        Receives a message over the WebSocket connection.

        Returns
        -------
        dict
            The received message, deserialized from JSON.
        """
        msg = self._socket.recv()
        msg = msg if msg != '' else '{}'
        return json.loads(msg)

    def run(self):
        """
        Runs the WebSocket client.
        This method tries to establish a connection and keep it alive.
        If an error occurs, it attempts to reestablish the connection after a delay.
        """
        self._connect()
        self._keep_alive()

        cnt = 0

        while cnt <= 10:
            print(f'INFO: Starting Loop #{cnt}/10')
            try:
                self._subscribe()
                self._loop()
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(5)
                cnt += 1
                self._connect()

    def _subscribe(self):
        """
        Placeholder method to be overridden in subclasses.
        This should handle subscription to a topic.
        """
        pass

    def _loop(self):
        """
        Placeholder method to be overridden in subclasses.
        This should handle incoming data.
        """
        pass
