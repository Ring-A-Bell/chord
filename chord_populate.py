"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Aditya Ganti
:Assignment: Lab 4
:EC Not attempted
"""
import csv
import hashlib
import os
import pickle
import socket
import sys

from chord_node import BASE_PORT, BUFF_SIZE, M
from typing import Union

BACKLOG = 100
NODES = 2 ** M
NROWS = 50


class PopulateChordNode:
    def __init__(self, node_id: int, file_name: str):
        """
        Constructor for the PopulateChordNode class

        :param node_id: ID of the node to which the data is being sent
        :param file_name: Name of the CSV file that contains the data
        """
        self.node_id = node_id
        self.file_name = file_name

        self.load_csv_file()

    def load_csv_file(self) -> None:
        """
        Checks for and loads the data file to populate the chord system.

        :return: None
        """
        try:
            # Check if the file exists in the current working directory
            if os.path.exists(self.file_name):

                # Load the CSV file into a pandas DataFrame
                # Omit the NROWS argument to load the entire file.
                # Currently set to 50 for ease-of-testing

                data = []
                with open(self.file_name, newline='') as f:
                    rows = csv.reader(f, delimiter=',', quotechar='"')
                    for row in rows:
                        data.append(row)
                        if rows.line_num == NROWS + 1:
                            break
                print("CSV file loaded successfully!\n\n")
                self.load_data_from_csv(data)
            else:
                print("File not found in the current directory.")
                return None
        except Exception as e:
            print("Error loading the CSV file:", e)
            return None

    def load_data_from_csv(self, loaded_data) -> None:
        """
        Helper function to load data into a Pandas DataFrame

        :param loaded_data:
        :return:
        """
        # If the CSV file is loaded successfully, you can perform operations on the loaded_data DataFrame
        if loaded_data is None:
            print("No data was loaded from the opened file")
            exit(0)

        print(loaded_data[0])
        loaded_data = loaded_data[1:]
        for row in loaded_data:
            # Example: Hash the key using playerId and year columns
            player_id = row[0]  # Assuming playerId is in the first column
            year = row[3]  # Assuming year is in the fourth column
            hashed_key = self.hash_key(player_id, year)

            print(f"Hashed key for Player Id {player_id} and year {year}: {hashed_key}")
            print(f"When this hash is modulo {NODES}, we get : {int(hashed_key, 16) % NODES}")
            print(f"Making an RPC call to node: {self.node_id} with this data")
            self.make_rpc_call(self.node_id, "ROUTE_DATA", hashed_key, row)
            print("------------------------------\n")

    @staticmethod
    def make_rpc_call(server_node: int, call_type: str, *params: Union[int, str, tuple]) -> None:
        """
        This function is responsible for marshalling an RPC call and sending it out.
        It also listens for a response and prints the requested resource.

        :param server_node: The node ID which is being called by the RPC request
        :param call_type: The resource that the RPC is requesting
        :param params: Payload sent in the RPC call
        :return: None
        """
        uri = "http://localhost:" + str(int(BASE_PORT) + server_node)
        print(f"Making a {call_type} RPC call to {server_node} at {uri}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', int(BASE_PORT + server_node)))
        sock.send(pickle.dumps((call_type, params)))
        print("Data sent to the node successfully!")
        data = pickle.loads(sock.recv(BUFF_SIZE))

    @staticmethod
    def hash_key(player_id: str, year: str) -> str:
        """
        Concatenate the arguments and hash the resulting string using SHA-1

        :param player_id: The player ID from the data file
        :param year: The year corresponding to the data row
        :return: SHA-1 hashed string
        """
        #
        key = f"{player_id}{year}".encode('utf-8')
        hashed_key = hashlib.sha1(key).hexdigest()
        return hashed_key


if __name__ == "__main__":
    """
    This program assumes the (BASE_PORT + n) way to lookup nodes.
    You are responsible for choosing an active node ID number.
    The data file should be present in the same directory as this file.
    The data file input is currently limited to 50 rows. Change the NROWS value as needed.

    Usage: python chord_populate.py NODE_ID FILE_NAME

    :param sys.argv (list): Command-line arguments provided when running the script.
    :raises SystemExit: Exits the program if the correct number of arguments is not provided.
    """
    if len(sys.argv) == 3:
        populateNode = PopulateChordNode(int(sys.argv[1]), sys.argv[2])
    else:
        print("Usage: python chord_populate.py NODE_ID FILE_NAME")
        exit(1)
