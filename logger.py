class Proc_Logger:
    def __init__(self):
        self.logging_filenames = []

    def create_peer_log(self, peer_id):
        """Create a log file for the given peer."""

        filename = f"proc_logging/peer_files/peer_{peer_id}/log_peer_{peer_id}.log"
        with open(filename, 'w') as f:
            f.write(f"")
            
        self.logging_filenames.append(filename)

    
        