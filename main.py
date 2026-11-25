"""Launch all peers in one process for the local prototype."""

import argparse
import logging
import shutil
import sys
import time
from pathlib import Path

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
    peer_ids = [peer["peer_id"] for peer in peers]
    clients = []

    try:
        for index, peer in enumerate(peers):
            # Local overrides leave the original configuration files intact.
            peer_id = peer["peer_id"]
            host = "127.0.0.1" if args.local else peer["host"]
            port = args.base_port + index if args.local else peer["port"]
            client = Client(peer_id, host, port, peer["has_file"], config)
            client.file_path.parent.mkdir(parents=True, exist_ok=True)
            if peer["has_file"]:
                source = args.config_dir / str(peer_id) / config["FileName"]
                shutil.copyfile(source, client.file_path)
            clients.append(client)
            client.start()

        # Every listener is ready before we establish the connections.
        # Each peer initiates only toward peers before it, avoiding duplicates.
        for index, client in enumerate(clients):
            for earlier_peer in clients[:index]:
                client.connect(earlier_peer.host, earlier_peer.port, earlier_peer.peer_id)

        logging.info("Started %s peers. Waiting for every peer to confirm completion.", len(clients))
        # Keep connections open until every peer has received the final availability updates.
        while True:
            for client in clients:
                if client.connection_error is not None:
                    raise client.connection_error
            if all(client.has_confirmed_completion(peer_ids) for client in clients):
                break
            time.sleep(0.1)
        logging.info("All %s peers have confirmed the complete file. Shutting down.", len(clients))

    except KeyboardInterrupt:
        logging.info("Stopping peers.")
    finally:
        for client in clients:
            client.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
