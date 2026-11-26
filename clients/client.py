import logging
import random
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from queue import Queue

from logger import Proc_Logger

from .messages import (
    bitfield_to_bytes,
    build_handshake,
    build_message,
    bytes_to_bitfield,
    parse_handshake,
    read_message,
    recv_exact,
)

logger = logging.getLogger(__name__)

CHOKE = 0
UNCHOKE = 1
INTERESTED = 2
NOT_INTERESTED = 3
HAVE = 4
BITFIELD = 5
REQUEST = 6
PIECE = 7


@dataclass
class Neighbor:
    bitfield: list[bool]
    outgoing: Queue[bytes | None] = field(default_factory=Queue)
    interested: bool = False
    interest_sent: bool | None = None
    unchoked_by: bool = False
    requested_piece: int | None = None
    downloaded_bytes: int = 0

    def send(self, message_type, payload=b""):
        self.outgoing.put(build_message(message_type, payload))


def shutdown_connection(conn):
    try:
        conn.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


class Client:
    """One peer, with a listener and a reader and writer per connection."""

    def __init__(self, peer_id, host, port, has_file, config):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.config = dict(config)
        peer_dir = Path("proc_logging") / "peer_files" / f"peer_{peer_id}"
        self.file_path = peer_dir / config["FileName"]
        self.peer_log = Proc_Logger(peer_dir / f"log_peer_{peer_id}.log")
        self.file_size = config["FileSize"]
        self.piece_size = config["PieceSize"]
        self.num_pieces = (self.file_size + self.piece_size - 1) // self.piece_size
        self.bitfield = [has_file] * self.num_pieces
        self.piece_count = self.num_pieces if has_file else 0
        self.has_file = has_file

        self.neighbors = {}
        self.preferred_neighbors = set()
        self.optimistic_neighbor = None

        self.listener_thread = None
        self.timer_thread = None
        self.connection_threads = []
        self.connection_error = None
        self.connections = set()
        self._server = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def start(self):
        """Prepare the file and start listening for other peers."""
        if self.listener_thread is not None or self._stop_event.is_set():
            raise RuntimeError("Create a new Client to start another listener")

        if self.has_file:
            if self.file_path.stat().st_size != self.file_size:
                raise ValueError(f"{self.file_path} does not match the configured FileSize")
        else:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with self.file_path.open("wb") as file:
                file.truncate(self.file_size)

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()
            server.settimeout(0.25)
            self.peer_log.path.write_text("", encoding="utf-8")
        except OSError:
            server.close()
            raise

        self._server = server
        self.port = server.getsockname()[1]
        self.listener_thread = threading.Thread(
            target=self._accept_connections, name=f"peer-{self.peer_id}-listener"
        )
        self.listener_thread.start()

        self.timer_thread = threading.Thread(
            target=self._neighbor_selection_loop, name=f"peer-{self.peer_id}-neighbor-timer"
        )
        self.timer_thread.start()

    def _accept_connections(self):
        while not self._stop_event.is_set():
            try:
                conn, addr = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self._stop_event.is_set():
                    logger.exception("Peer %s listener failed", self.peer_id)
                break
            self._start_connection(conn, addr, initiated=False, expected_id=None)

    def connect(self, host, port, expected_id):
        conn = socket.create_connection((host, port), timeout=3)
        conn.settimeout(None)
        self._start_connection(conn, conn.getpeername(), initiated=True, expected_id=expected_id)

    def _start_connection(self, conn, addr, initiated, expected_id):
        with self._lock:
            if self._stop_event.is_set():
                conn.close()
                return

            thread = threading.Thread(
                target=self._run_connection,
                args=(conn, addr, initiated, expected_id),
                name=f"peer-{self.peer_id}-connection-{addr[0]}:{addr[1]}",
            )
            self.connections.add(conn)
            self.connection_threads.append(thread)
            thread.start()

    def _run_connection(self, conn, addr, initiated, expected_id):
        try:
            self.handle_connection(conn, addr, initiated, expected_id)
        except FileNotFoundError as error:
            self.connection_error = error

    def handle_connection(self, conn, addr, initiated, expected_id):
        other_id = None
        neighbor = None
        writer = None
        try:
            conn.sendall(build_handshake(self.peer_id))
            other_id = parse_handshake(recv_exact(conn, 32))
            if other_id == self.peer_id or (expected_id is not None and other_id != expected_id):
                raise ValueError(f"unexpected peer ID {other_id}")

            direction = "makes a connection to" if initiated else "is connected from"
            message = f"Peer {self.peer_id} {direction} Peer {other_id}."
            with self._lock:
                if other_id in self.neighbors:
                    raise ValueError(f"duplicate connection to peer {other_id}")
                self.peer_log.append(message)
                neighbor = Neighbor([False] * self.num_pieces)
                if self.piece_count:
                    neighbor.send(BITFIELD, bitfield_to_bytes(self.bitfield))
                self.neighbors[other_id] = neighbor

            writer = threading.Thread(
                target=self._write_messages,
                args=(conn, neighbor.outgoing),
                name=f"peer-{self.peer_id}-writer-{other_id}",
            )
            writer.start()

            logger.info("%s", message)

            while not self._stop_event.is_set():
                msg_type, payload = read_message(conn)
                self.handle_message(msg_type, payload, other_id)

        except ConnectionError:
            logger.info("Peer %s disconnected from %s", self.peer_id, other_id)
        except FileNotFoundError:
            raise
        except (OSError, ValueError):
            if not self._stop_event.is_set():
                logger.exception("Peer %s connection to %s failed", self.peer_id, addr)
        finally:
            with self._lock:
                self.connections.discard(conn)
                if neighbor is not None:
                    del self.neighbors[other_id]
                    self.preferred_neighbors.discard(other_id)
                    if self.optimistic_neighbor == other_id:
                        self.optimistic_neighbor = None
                    if neighbor.requested_piece is not None:
                        self._resume_waiting_downloads()

            shutdown_connection(conn)
            if writer is not None:
                neighbor.outgoing.put(None)
                writer.join()
            conn.close()

    @staticmethod
    def _write_messages(conn, outgoing):
        # A single writer preserves protocol order without holding the peer lock during sendall.
        try:
            while (message := outgoing.get()) is not None:
                conn.sendall(message)
        except OSError:
            shutdown_connection(conn)

    def handle_message(self, msg_type, payload, other_id):
        with self._lock:
            neighbor = self.neighbors[other_id]

            if msg_type == BITFIELD:
                if len(payload) != (self.num_pieces + 7) // 8:
                    raise ValueError("invalid bitfield length")
                neighbor.bitfield = bytes_to_bitfield(payload, self.num_pieces)
                self._update_interest(neighbor)
                self._request_piece(neighbor)

            elif msg_type == HAVE:
                index = self._read_index(payload)
                neighbor.bitfield[index] = True
                logger.info("Peer %s received the 'have' message from %s for the piece %s.",
                            self.peer_id, other_id, index)
                self._update_interest(neighbor)
                self._request_piece(neighbor)

            elif msg_type == INTERESTED or msg_type == NOT_INTERESTED:
                neighbor.interested = msg_type == INTERESTED
                interest = "interested" if neighbor.interested else "not interested"
                logger.info("Peer %s received the '%s' message from %s.",
                            self.peer_id, interest, other_id)

            elif msg_type == CHOKE:
                neighbor.unchoked_by = False
                logger.info("Peer %s is choked by %s.", self.peer_id, other_id)
                if neighbor.requested_piece is not None:
                    neighbor.requested_piece = None
                    self._resume_waiting_downloads()

            elif msg_type == UNCHOKE:
                neighbor.unchoked_by = True
                logger.info("Peer %s is unchoked by %s.", self.peer_id, other_id)
                self._request_piece(neighbor)

            elif msg_type == REQUEST:
                index = self._read_index(payload)
                if self.bitfield[index] and other_id in self._unchoked_neighbors():
                    data = self.read_piece(index)
                    if len(data) != self._piece_length(index):
                        raise OSError(f"could not read the complete piece {index} from {self.file_path}")
                    neighbor.send(PIECE, payload + data)

            elif msg_type == PIECE:
                self._receive_piece(other_id, neighbor, payload)

    def _read_index(self, payload):
        if len(payload) != 4:
            raise ValueError("piece index must be four bytes")
        index = int.from_bytes(payload, "big")
        if index >= self.num_pieces:
            raise ValueError("invalid piece index")
        return index

    def _receive_piece(self, other_id, neighbor, payload):
        index = self._read_index(payload[:4])
        if index != neighbor.requested_piece:
            return

        data = payload[4:]
        if len(data) != self._piece_length(index):
            raise ValueError(f"incorrect length for piece {index}")

        self.write_piece(index, data)
        neighbor.requested_piece = None
        neighbor.downloaded_bytes += len(data)
        self.bitfield[index] = True
        self.piece_count += 1
        logger.info(
            "Peer %s has downloaded the piece %s from peer %s. Now the number of pieces it has is %s.",
            self.peer_id, index, other_id, self.piece_count,
        )

        for connected_neighbor in self.neighbors.values():
            connected_neighbor.send(HAVE, payload[:4])
            self._update_interest(connected_neighbor)

        if self.piece_count == self.num_pieces:
            self.has_file = True
            logger.info("Peer %s has downloaded the complete file.", self.peer_id)

        self._request_piece(neighbor)

    def _update_interest(self, neighbor):
        interested = any(remote and not own for own, remote in zip(self.bitfield, neighbor.bitfield))
        if interested != neighbor.interest_sent:
            neighbor.send(INTERESTED if interested else NOT_INTERESTED)
            neighbor.interest_sent = interested

    def _resume_waiting_downloads(self):
        if not self._stop_event.is_set():
            for neighbor in self.neighbors.values():
                self._request_piece(neighbor)

    def _request_piece(self, neighbor):
        if not neighbor.unchoked_by or not neighbor.interest_sent or neighbor.requested_piece is not None:
            return

        reserved = {remote.requested_piece for remote in self.neighbors.values()}
        candidates = [
            index for index, available in enumerate(neighbor.bitfield)
            if available and not self.bitfield[index] and index not in reserved
        ]
        if candidates:
            index = random.choice(candidates)
            neighbor.requested_piece = index
            neighbor.send(REQUEST, index.to_bytes(4, "big"))

    def _unchoked_neighbors(self):
        if self.optimistic_neighbor is None:
            return self.preferred_neighbors
        return self.preferred_neighbors | {self.optimistic_neighbor}

    def _send_choke_changes(self, previously_unchoked):
        unchoked = self._unchoked_neighbors()
        for peer_id in previously_unchoked - unchoked:
            self.neighbors[peer_id].send(CHOKE)
        for peer_id in unchoked - previously_unchoked:
            self.neighbors[peer_id].send(UNCHOKE)

    def _select_preferred_neighbors(self):
        candidates = [peer_id for peer_id, neighbor in self.neighbors.items() if neighbor.interested]
        random.shuffle(candidates)
        if not self.has_file:
            # Each rate covers the same interval, so byte totals have the same ordering.
            candidates.sort(key=lambda peer_id: self.neighbors[peer_id].downloaded_bytes, reverse=True)
        for neighbor in self.neighbors.values():
            neighbor.downloaded_bytes = 0

        chosen = set(candidates[:self.config["NumberOfPreferredNeighbors"]])
        if chosen != self.preferred_neighbors:
            previously_unchoked = self._unchoked_neighbors()
            self.preferred_neighbors = chosen
            self._send_choke_changes(previously_unchoked)
            logger.info("Peer %s has the preferred neighbors %s.",
                        self.peer_id, ",".join(str(peer_id) for peer_id in sorted(chosen)))

    def _select_optimistic_neighbor(self):
        previously_unchoked = self._unchoked_neighbors()
        candidates = [
            peer_id for peer_id, neighbor in self.neighbors.items()
            if neighbor.interested and peer_id not in previously_unchoked
        ]
        chosen = random.choice(candidates) if candidates else None
        if chosen != self.optimistic_neighbor:
            self.optimistic_neighbor = chosen
            self._send_choke_changes(previously_unchoked)
            if chosen is not None:
                logger.info("Peer %s has the optimistically unchoked neighbor %s.", self.peer_id, chosen)

    def _neighbor_selection_loop(self):
        preferred_interval = self.config["UnchokingInterval"]
        optimistic_interval = self.config["OptimisticUnchokingInterval"]
        now = time.monotonic()
        preferred_at = now + preferred_interval
        optimistic_at = now + optimistic_interval
        while not self._stop_event.wait(max(0, min(preferred_at, optimistic_at) - time.monotonic())):
            with self._lock:
                now = time.monotonic()
                if now >= preferred_at:
                    self._select_preferred_neighbors()
                    preferred_at = now + preferred_interval
                if now >= optimistic_at:
                    self._select_optimistic_neighbor()
                    optimistic_at = now + optimistic_interval

    def has_confirmed_completion(self, peer_ids):
        """Check our file and the availability announced by every other peer."""
        with self._lock:
            return self.has_file and all(
                peer_id in self.neighbors and all(self.neighbors[peer_id].bitfield)
                for peer_id in peer_ids if peer_id != self.peer_id
            )

    def stop(self):
        """Wake the listener, timer, and connection workers and wait for them."""
        self._stop_event.set()
        if self._server is not None:
            self._server.close()

        with self._lock:
            connections = list(self.connections)
            threads = list(self.connection_threads)

        for conn in connections:
            shutdown_connection(conn)

        if self.listener_thread is not None:
            self.listener_thread.join()
        if self.timer_thread is not None:
            self.timer_thread.join()
        for thread in threads:
            thread.join()

    def _piece_length(self, index):
        return min(self.piece_size, self.file_size - index * self.piece_size)

    def read_piece(self, index):
        with self.file_path.open("rb") as file:
            file.seek(index * self.piece_size)
            return file.read(self._piece_length(index))

    def write_piece(self, index, data):
        with self.file_path.open("r+b") as file:
            file.seek(index * self.piece_size)
            file.write(data)
