import logging
from logger import Proc_Logger
import socket
import threading
import time
import math
import random

from .messages import (build_handshake, parse_handshake, recv_exact, 
                       build_message, read_message, bitfield_to_bytes,
                       bytes_to_bitfield)

logger = logging.getLogger(__name__)

#msg types
CHOKE = 0
UNCHOKE = 1
INTERESTED = 2
NOT_INTERESTED = 3
HAVE = 4
BITFIELD = 5
REQUEST = 6
PIECE = 7

class Client:
    """One prototype peer, with a listener and a worker per connection."""

    def __init__(self, peer_id, host, port, has_file, config):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.has_file = has_file
        self.config = dict(config)

        # calculate the number of pieces based on file size and piece size and set the bit field
        self.num_pieces = math.ceil(config["FileSize"] / config["PieceSize"])
        # [True, False, True]
        self.bitfield = [has_file] * self.num_pieces 

        # key is peer id and value is the bitfield of that peer as a list
        self.neighbor_bitfields = {}

        # keep track of neighbors that are interested and preferred neighbors
        self.interested_neighbors = set()
        self.preferred_neighbors = set()

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

     

        # logging.info("Peer %s listening on %s:%s", self.peer_id, self.host, self.port)

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
            self._start_connection(conn, addr, initiated=False, expected_id=None)

    def connect(self, host, port, expected_id):
        """Use the same worker for an outgoing socket as for an accepted one."""
        conn = socket.create_connection((host, port), timeout=3)
        conn.settimeout(None)
        self._start_connection(conn, conn.getpeername(), initiated=True, expected_id=expected_id)

    def _start_connection(self, conn, addr, initiated, expected_id):
        with self._lock:

            if self._stop_event.is_set():
                conn.close()
                return
            
            thread = threading.Thread(
                target=self.handle_connection,
                args=(conn, addr, initiated, expected_id),
                name=f"peer-{self.peer_id}-connection-{addr[0]}:{addr[1]}",
            )

            self.connections[addr] = conn
            self.connection_threads.append(thread)
            thread.start()

    def handle_connection(self, conn, addr, initiated, expected_id):
        try:
            with conn:
                #send handshake message
                conn.sendall(build_handshake(self.peer_id))

                # read other peer's handshake
                data = recv_exact(conn, 32)
                other_id = parse_handshake(data)
                # validate
                if expected_id is not None and other_id != expected_id:
                    raise ValueError(f"expected peer {expected_id}, got {other_id}")

                # this logs the inital connection messages
                Logger = Proc_Logger()
                Logger.log_TCP_connection(time.time(), self.peer_id, other_id)

                # initialize this neighbors bitfield to nothing
                self.neighbor_bitfields[other_id] = [False] * self.num_pieces

                # log connection
                if initiated:
                    logging.info("Peer %s makes a connection to Peer %s.", self.peer_id, other_id)
                else:
                    logging.info("Peer %s is connected from Peer %s.", self.peer_id, other_id)

                # send bitfield to other peer if self has any pieces
                if any(self.bitfield):
                    conn.sendall(build_message(BITFIELD, bitfield_to_bytes(self.bitfield)))
                
                while not self._stop_event.is_set():
                    msg_type, payload = read_message(conn)
                    self.handle_message(conn, msg_type, payload, other_id)

        except (OSError, ValueError):
            if not self._stop_event.is_set():
                logging.exception("Peer %s connection to %s failed", self.peer_id, addr)
        
        finally:
            with self._lock:
                if self.connections.get(addr) is conn:
                    del self.connections[addr]
            logging.info("Peer %s disconnected from %s", self.peer_id, addr)

    def handle_message(self, conn, msg_type, payload, other_id):
        """Placeholder: a recv chunk is not necessarily a complete message."""
        logging.info("Peer %s got message type %s (%s bytes) from %s", self.peer_id, msg_type, len(payload), other_id)
        
        # handle a bitfield message
        if msg_type == BITFIELD:
            others_bitfield = bytes_to_bitfield(payload, self.num_pieces)

            # lock dictionary so that threads don't overwrite
            with self._lock:
                self.neighbor_bitfields[other_id] = others_bitfield

            if self.neighbor_has_something_i_need(others_bitfield):
                conn.sendall(build_message(INTERESTED))
            else:
                conn.sendall(build_message(NOT_INTERESTED))

        elif msg_type == INTERESTED:
            # if recieving an interested message, add that peer to the interested neighbors set
            with self._lock:
                self.interested_neighbors.add(other_id)
            logging.info("Peer %s received the 'interested' message from %s.", self.peer_id, other_id)

        elif msg_type == NOT_INTERESTED:
            # if recieving a not interested message, add that peer to the interested neighbors set
            with self._lock:
                self.interested_neighbors.discard(other_id)
            logging.info("Peer %s received the 'not interested' message from %s.", self.peer_id, other_id)


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

    # helper establishing interest
    def neighbor_has_something_i_need(self, thiers):
        for i in range(self.num_pieces):
            if thiers[i] and not self.bitfield[i]:
                return True
            else:
                return False

    def select_preferred_neighbors(self):
        k = self.config["NumberOfPreferredNighbors"]

        with self._lock:
            candidates = list(self.interested_neighbors)

        # if the peer has the whole file, randomly select k neighbors for unchoking
        if all(self.bitfield):
            return set(random.sample(candidates, min(k, len(candidates))))

        #TODO: handle selecting preferred neighbors when its not just completely random
        return set()

