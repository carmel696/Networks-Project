# 1. initalizes peers, establishes connections between them

def main():
    config_dict = {}

    with open("project_config_file_small/Common.cfg", "r") as file:
        config = file.readlines()

        for line in config:
            line = line.split()
            config_dict[line[0]] = line[1]

    # print(config_dict)

    peer_dict = {}
    with open("project_config_file_small/PeerInfo.cfg", "r") as file:
        peer_info = file.readlines()

        for peer in peer_info:
            peer = peer.split()
            print(peer)
            peer_dict[peer[0]] = {peer[1], peer[2], peer[3]}

    print(peer_dict)

    # for line in peer_info:
    #     line = line.split()
    #     client_dict[line[0]] = line[1]

    # print(client_dict)



if __name__ == "__main__":
    main()