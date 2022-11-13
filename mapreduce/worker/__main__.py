"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.registered = False
        self.working = True

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            
            register_message = {
                "message_type": "register",
                "worker_host": self.host,
                "worker_port": self.port,   
            }
            mapreduce.utils.send_TCP_message(self.manager_host, self.manager_port, register_message)

            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            
            while self.working:
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])

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

                thread1 = threading.Thread(target=self.heartbeat,)
                if message_dict["message_type"] == "shutdown":
                    self.working = False
                elif message_dict["message_type"] == "register_ack":
                    self.registered = True
                    thread1.start()
                elif message_dict["message_type"] == "new_map_task":
                    self.mapping(message_dict)
                elif message_dict["message_type"] == "new_reduce_task":
                    self.reducing(message_dict)

            if self.registered:
                thread1.join()



        # while self.working:
        #     message_dict = mapreduce.utils.create_TCP(host, port)
        #     if not self.registered:
            # if message_dict["message_type"] == "shutdown":
            #     self.shutdown(message_dict)
            #     self.working = False
            # elif message_dict["message_type"] == "register":
            #     self.register(message_dict)
            # elif message_dict["message_type"] == "new_manager_job":
            #     self.new_manager_job(message_dict)
            # elif message_dict["message_type"] == "finished":
            #     self.finished(message_dict)
            # elif message_dict["message_type"] == "heartbeat":
            #     self.heartbeat(message_dict)
            # if not self.is_running_job:
            #     self.run_job()

        # Create a new TCP socket on the given port and call the listen() function. 
        # Note: only one listen() thread should remain open for the whole lifetime of the Manager.

        # thread = threading.Thread(target=self.recv_msg())
        # thread.start()
        # self.register()
        # thread.join()

    
    # def recv_msg(self):
    #     thread = threading.Thread(target=self.heartbeat())
    #     while self.working:
    #         message_dict = mapreduce.utils.create_TCP(self.host, self.port)
    #         if message_dict["message_type"] == "shutdown":
    #             self.working = False
    #         elif message_dict["message_type"] == "register_ack":
    #             thread.start()
    #         elif message_dict["message_type"] == "new_map_task":
    #             self.mapping(message_dict)
    #         elif message_dict["message_type"] == "new_reduce_task":
    #             self.reducing(message_dict)
    #     thread.join()


    # def register(self):
    #     info = {
    #         "message_type": "register",
    #         "worker_host": self.host,
    #         "worker_port": self.port,
    #     }
    #     mapreduce.utils.send_TCP_message(self.manager_host, self.manager_port, info)

    def heartbeat(self):
        while self.working:
            heartbeat = {
                "message_type": "heartbeat",
                "worker_host": self.host,
                "worker_port": self.port,
            }
            mapreduce.utils.send_UDP_message(self.manager_host, self.manager_port, heartbeat)
            time.sleep(2000)


    def mapping(self):
        ## TODO ##
        return

    def reducing(self):
        ## TODO ##
        return


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
