class Proc_Logger:
    def __init__(self):
        self.logging_dict = {}

    def create_peer_log(self, peer_id):
        """Create a log file for the given peer."""

        filename = f"proc_logging/peer_files/peer_{peer_id}/log_peer_{peer_id}.log"
        with open(filename, 'w') as f:
            f.write(f"")

        self.logging_dict[peer_id] = filename


    def log_TCP_connection(self, time, primary_peer_id, connected_peer_id):
        self.create_peer_log(primary_peer_id) # shouldn't be neccesary but im too tired to go check 
        self.create_peer_log(connected_peer_id)

        primary_peer_filename = self.logging_dict[primary_peer_id]
        # print("HI", primary_peer_filename)
        with open(primary_peer_filename, 'a') as f:
            f.write(f"{time}: Peer {primary_peer_id} makes a connection to Peer {connected_peer_id}\n")

        connected_peer_filename = self.logging_dict[connected_peer_id]

        with open(connected_peer_filename, 'a') as f:
            f.write(f"{time}: Peer {connected_peer_id} makes a connection from Peer {primary_peer_id}\n")

        
