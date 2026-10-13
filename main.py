"""Launch all peers in one process for the local prototype."""

import argparse
import logging
from pathlib import Path
import sys
import threading
import os


from clients.client import Client


def load_config(config_dir):
    config = {}
    with (config_dir / "Common.cfg").open() as file:
        for line in file:
            if line.strip():
                key, value = line.split()
                config[key] = value if key == "FileName" else int(value)

    # A list preserves the startup order required by PeerInfo.cfg.
    peers = []
    with (config_dir / "PeerInfo.cfg").open() as file:
        for line in file:
            if line.strip():
                peer_id, host, port, has_file = line.split()
                peers.append({
                    "peer_id": int(peer_id),
                    "host": host,
                    "port": int(port),
                    "has_file": bool(int(has_file)),
                })
    return config, peers



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config-dir", type=Path,
        default=Path(__file__).resolve().parent / "project_config_file_small",
    )
    parser.add_argument(
        "--local", action="store_true",
        help="Use localhost and distinct ports instead of the configured hosts/ports",
    )
    parser.add_argument("--base-port", type=int, default=6001)
    args = parser.parse_args()


    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    config, peers = load_config(args.config_dir)
    clients = []

    try:
        for index, peer in enumerate(peers):
            # initalize peer directories for fille storage
            os.makedirs(f"logging/peer_{peer["peer_id"]}", exist_ok=True)

            # Local overrides leave the original configuration files intact.
            host = "127.0.0.1" if args.local else peer["host"]
            port = args.base_port + index if args.local else peer["port"]
            client = Client(peer["peer_id"], host, port, peer["has_file"], config)
            clients.append(client)
            client.start()

        # Every listener is ready before we establish the connections.
        # Each peer initiates only toward peers before it, avoiding duplicates.
        for index, client in enumerate(clients):
            for earlier_peer in clients[:index]:
                client.connect(earlier_peer.host, earlier_peer.port, earlier_peer.peer_id)

        logging.info("Started %s peers. Press Ctrl+C to stop.", len(clients))
        # TODO: replace this wait with detection that all peers have the file.
        threading.Event().wait()
    except KeyboardInterrupt:
        # ctr c
        logging.info("Stopping peers.")
    finally:
        for client in clients:
            client.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
