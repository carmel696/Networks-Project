import logging
import socket
import threading
from .messages import build_handshake, parse_handshake, recv_exact

class Client:
    """One prototype peer, with a listener and a worker per connection."""

    def __init__(self, peer_id, host, port, has_file, config):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.has_file = has_file
        self.config = dict(config)

        # TODO: choose a structure for preferred neighbors and choking state.
        # list? idk if order matters
        self.preferred_neighbors = None

        self.listener_thread = None
        self.connection_threads = []
        # Addresses are temporary keys until the handshake identifies each peer.
        self.connections = {}
        self._server = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def start(self):
        """Start listening and return so main can start the other peers."""
        if self.listener_thread is not None or self._stop_event.is_set():
            raise RuntimeError("Create a new Client to start another listener")

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()
            server.settimeout(0.25)
        except OSError:
            server.close()
            raise

        self._server = server
        self.port = server.getsockname()[1]
        self.listener_thread = threading.Thread(
            target=self._accept_connections,
            name=f"peer-{self.peer_id}-listener",
        )
        self.listener_thread.start()
        logging.info("Peer %s listening on %s:%s", self.peer_id, self.host, self.port)

    def _accept_connections(self):
        while not self._stop_event.is_set():
            try:
                conn, addr = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self._stop_event.is_set():
                    logging.exception("Peer %s listener failed", self.peer_id)
                break
            self._start_connection(conn, addr, initiated=False)

    def connect(self, host, port):
        """Use the same worker for an outgoing socket as for an accepted one."""
        conn = socket.create_connection((host, port), timeout=3)
        conn.settimeout(None)
        self._start_connection(conn, conn.getpeername(), initiated=True)

    def _start_connection(self, conn, addr, initiated):
        with self._lock:

            if self._stop_event.is_set():
                conn.close()
                return
            
            thread = threading.Thread(
                target=self.handle_connection,
                args=(conn, addr, initiated),
                name=f"peer-{self.peer_id}-connection-{addr[0]}:{addr[1]}",
            )

            self.connections[addr] = conn
            self.connection_threads.append(thread)
            thread.start()

    def handle_connection(self, conn, addr, initiated):
        try:
            with conn:
                #send handshake message
                conn.sendall(build_handshake(self.peer_id))
                
                # read other peer's handshake
                data = recv_exact(conn, 32)

                other_id = parse_handshake(data)

                if initiated:
                    logging.info("Peer %s makes a connection to Peer %s.", self.peer_id, other_id)
                else:
                    logging.info("Peer %s is connected from Peer %s.", self.peer_id, other_id)
                
                while not self._stop_event.is_set():
                    data = conn.recv(4096)
                    if not data:
                        break
                    self.handle_data(data, addr)
        except OSError:
            if not self._stop_event.is_set():
                logging.exception("Peer %s connection to %s failed", self.peer_id, addr)
        finally:
            with self._lock:
                if self.connections.get(addr) is conn:
                    del self.connections[addr]
            logging.info("Peer %s disconnected from %s", self.peer_id, addr)

    def handle_data(self, data, addr):
        """Placeholder: a recv chunk is not necessarily a complete message."""
        logging.info("Peer %s received %s bytes from %s", self.peer_id, len(data), addr)
        # TODO: dispatch messages and update piece/preferred-neighbor state.

    def stop(self):
        """Wake blocked socket workers and wait for all tracked threads."""
        self._stop_event.set()
        if self._server is not None:
            self._server.close()

        with self._lock:
            connections = list(self.connections.values())
            threads = list(self.connection_threads)

        for conn in connections:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()

        if self.listener_thread is not None:
            self.listener_thread.join()
        for thread in threads:
            thread.join()
