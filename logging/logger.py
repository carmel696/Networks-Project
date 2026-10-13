class Logger(object):
    def __init__(self):
        logging_files = []

    def create_peer_log(self, peer_id):
        """Create a log file for the given peer."""
        filename = f"peer_{peer_id}.log"

        with open('w', filename) as f:
            f.write(f"Peer {peer_id} log file created.\n")
            


        self.logging_files.append(filename)

        
        