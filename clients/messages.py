HEADER = b"P2PFILESHARINGPROJ"
ZEROS = b"\x00" * 10

def build_handshake(peer_id):
    peer_id_bytes = (peer_id).to_bytes(4, "big")
    return HEADER + ZEROS + peer_id_bytes

def parse_handshake(handshake):

    if len(handshake) != 32 or handshake[:18] != HEADER:
        raise ValueError("invalid handshake")
    # return the last 4 bytes as the int peer_id it is
    return int.from_bytes(handshake[28:32], "big")

def recv_exact(conn, n):
    data = b"" 
    while len(data) < n:
        chunk = conn.recv(n-len(data))

        if not chunk:
            raise ConnectionError("connection closed before all bytes arrived")

        data += chunk

    return data
  
def build_message(msg_type, payload=b""):
    # convert to bytes to send
    length = 1 + len(payload)
    return length.to_bytes(4, "big") + bytes([msg_type]) + payload

def read_message(conn):
    length = int.from_bytes(recv_exact(conn, 4), "big")
    body = recv_exact(conn, length)
    return body[0], body[1:]

def bitfield_to_bytes(bitfield):
    data = bytearray((len(bitfield) + 7) // 8)

    for i, has_piece in enumerate(bitfield):
        if has_piece:
            data[i // 8] |= 1 << (7 - i % 8)

    return bytes(data)

def bytes_to_bitfield(data, num_pieces):
    bitfield = []
    for i in range(num_pieces):
        bitfield.append(bool(data[i // 8] & (1 << (7 - i % 8))))

    return bitfield
