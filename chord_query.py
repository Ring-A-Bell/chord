"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Aditya Ganti
:Assignment: Lab 4
:EC Not attempted
"""
import pickle
import socket
import sys

from chord_node import BASE_PORT, BUFF_SIZE, M

NODES = 2 ** M
BACKLOG = 100


class QueryChordNode:
    def __init__(self, node_id: int, query_key: str):
        self.node_id = node_id
        self.query_key = query_key

        self.dispatch_query()

    def dispatch_query(self) -> None:
        """
        Helper function that just calls the Make_RPC_Call function

        :return: None
        """
        self.make_rpc_call(self.node_id, "QUERY", self.query_key)

    @staticmethod
    def make_rpc_call(server_node: int, call_type: str, *params: str) -> None:
        """
        This function is responsible for marshalling an RPC call and sending it out.
        It also listens for a response and prints the requested resource.

        :param server_node: The node ID which is being called by the RPC request
        :param call_type: The resource that the RPC is requesting
        :param params: Payload sent in the RPC call
        :return: None
        """
        uri = "http://localhost:" + str(BASE_PORT + server_node)
        print(f"Making a {call_type} RPC call to {server_node} at {uri}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', int(BASE_PORT + server_node)))
        sock.send(pickle.dumps((call_type, params)))
        print("Data sent to the node successfully!\n\n")
        data = pickle.loads(sock.recv(BUFF_SIZE))

        print("-----------------------------------")

        print(f"The key was found at node: {data[0]}")
        print(f"Here is the data that has been returned:\n{data[1]}")


if __name__ == "__main__":
    """
    This program assumes the (BASE_PORT + n) way to lookup nodes.
    You are responsible for choosing an active node ID number.
    The key should be a valid SHA-1 hash value. Choose a key that exists
    in the network, otherwise you might end up with an infinite loop

    Usage: python chord_populate.py NODE_ID HASHED_KEY_IN_HEX

    :param sys.argv (list): Command-line arguments provided when running the script.
    :raises SystemExit: Exits the program if the correct number of arguments is not provided.
    """
    if len(sys.argv) == 3:
        query = QueryChordNode(int(sys.argv[1]), sys.argv[2])
    else:
        print("Usage: python chord_populate.py NODE_ID HASHED_KEY_IN_HEX")
        exit(1)
