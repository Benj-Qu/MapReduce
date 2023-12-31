"""UTILS functions for use in manager and worker classes."""
import socket
import json
import time


def create_tcp(self, send_msg):
    """Create a new tcp socket on the given port."""
    # Create an INET, STREAMing socket, this is tcp
    # Note: context manager syntax allows for sockets to automatically be
    # closed when an exception is raised or control flow returns.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.vars['host'], self.vars['port']))
        sock.listen()

        if send_msg is not None:
            send_tcp_message(
                self.vars['manager_host'],
                self.vars['manager_port'],
                send_msg
                )

        # Socket accept() will block for a maximum of 1 second.  If you
        # omit this, it blocks indefinitely, waiting for a connection.
        sock.settimeout(1)

        while self.get_working():
            try:
                clientsocket, _ = sock.accept()
            except socket.timeout:
                continue

            # Socket recv() will block for a maximum of 1 second.  If you omit
            # this, it blocks indefinitely, waiting for packets.
            clientsocket.settimeout(1)

            # Receive data, one chunk at a time.  If recv() times out before we
            # can read a chunk, then go back to the top of the loop and try
            # again.  When the client closes the connection, recv() returns
            # empty data, which breaks out of the loop.  We make a simplifying
            # assumption that the client will always cleanly close the
            # connection.
            with clientsocket:
                message_chunks = []
                while True:
                    try:
                        data = clientsocket.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message_chunks.append(data)

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")

            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            self.tcp_handler(message_dict)


def send_tcp_message(host, port, message_dict):
    """Test tcp Socket Client."""
    # create an INET, STREAMing socket, this is tcp
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            # connect to the server
            sock.connect((host, port))
            # send a message
            message = json.dumps(message_dict)
            sock.sendall(message.encode('utf-8'))
        except ConnectionRefusedError:
            return False
    return True


def create_udp(self, host, port):
    """Test udp Socket Server."""
    # Create an INET, DGRAM socket, this is udp
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Bind the udp socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.settimeout(1)

        # No sock.listen() since udp doesn't establish connections like tcp

        # Receive incoming udp messages
        while self.get_working():
            time.sleep(0.1)
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)
            if message_dict["message_type"] == "heartbeat":
                self.udp_handler(message_dict)


def send_udp_message(host, port, message_dict):
    """Test udp Socket Client."""
    # Create an INET, DGRAM socket, this is udp
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        # Connect to the udp socket on server
        sock.connect((host, port))
        # Send a message
        message = json.dumps(message_dict)
        sock.sendall(message.encode('utf-8'))


def get_working(self):
    """Get whether the class is working."""
    with self.lock:
        working = self.vars["working"]
    return working
