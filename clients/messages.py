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
        
