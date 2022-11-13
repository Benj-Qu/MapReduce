"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket
from pathlib import Path


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        self.host = host
        self.port = port

        self.workers = {}
        self.working = True
        self.jobs = [] # job queue, add by append, remove by self.jobs.pop(0)
        self.num_jobs = 0 # assign job id
        self.is_running_job = False

        self.create_TCP = mapreduce.utils.create_TCP

        # Create a new thread, which will listen for UDP heartbeat messages from the Workers.
        threads = []
        thread1 = threading.Thread(target=self.listen_worker_heartbeat, args=(host, port,))
        threads.append(thread1)
        thread1.start()
        # Create any additional threads or setup you think you may need. 
        # Another thread for fault tolerance could be helpful.
        thread2 = threading.Thread(target=self.fault_tolerance, args=(host, port,))
        threads.append(thread2)
        thread2.start()
        # Create a new TCP socket on the given port and call the listen() function. 
        # Note: only one listen() thread should remain open for the whole lifetime of the Manager.
        self.create_TCP(self, None)
            
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            sock.settimeout(1)
            while self.working:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])
                clientsocket.settimeout(1)
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
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue

                if message_dict["message_type"] == "shutdown":
                    self.shutdown(message_dict)
                    self.working = False
                elif message_dict["message_type"] == "register":
                    self.register(message_dict)
                elif message_dict["message_type"] == "new_manager_job":
                    self.new_manager_job(message_dict)
                elif message_dict["message_type"] == "finished":
                    self.finished(message_dict)
                elif message_dict["message_type"] == "heartbeat":
                    self.heartbeat(message_dict)
                if not self.is_running_job:
                    self.run_job()

        thread1.join()
        thread2.join()

    def handler(self, message_dict):
        if message_dict["message_type"] == "shutdown":
            self.shutdown(message_dict)
            self.working = False
        elif message_dict["message_type"] == "register":
            self.register(message_dict)
        elif message_dict["message_type"] == "new_manager_job":
            self.new_manager_job(message_dict)
        elif message_dict["message_type"] == "finished":
            self.finished(message_dict)
        elif message_dict["message_type"] == "heartbeat":
            self.heartbeat(message_dict)
        if not self.is_running_job:
            self.run_job()




    def listen_worker_heartbeat(self, host, port):
        # Listen for UDP heartbeat messages from the workers
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.settimeout(1)

            # No sock.listen() since UDP doesn't establish connections like TCP

            # TODO: IMPLEMENT THIS
            # Receive incoming UDP messages
            while self.working:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                print(message_dict)


    def fault_tolerance(self, host, port):
        # TODO: IMPLEMENT THIS
        # Listen for UDP heartbeat messages from the workers
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.settimeout(1)

            # No sock.listen() since UDP doesn't establish connections like TCP

            # TODO: IMPLEMENT THIS
            # Receive incoming UDP messages
            while self.working:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                print(message_dict)


    def shutdown(self, message_dict):
        # Forward this message to all of the living Workers
        # that have registered with it
        for host, port in self.workers.keys():
            mapreduce.utils.send_TCP_message(host, port, message_dict)


    def register(self, message_dict):
        # Register a worker
        self.workers[(message_dict["worker_host"], message_dict["worker_port"])] = "ready"
        message_dict["message_type"] = "register_ack"
        mapreduce.utils.send_TCP_message(message_dict["worker_host"], message_dict["worker_port"], message_dict)
        # check job queue


    def new_manager_job(self, message_dict):
        # Assign a job_id which starts from zero and increments.
        message_dict["job_id"] = self.num_jobs
        self.num_jobs += 1
        # Add the job to a queue.
        self.jobs.append(message_dict)
        # Delete the output directory if it exists. Create the output directory.
        output_directory_path = Path(message_dict["output_directory"])
        if output_directory_path.exists():
            output_directory_path.rmdir()
        output_directory_path.mkdir()


    def partition(self, directory):
        p = Path(directory).glob('**/*')
        files = [x for x in p if x.is_file()]


    def run_job(self):
        if self.jobs:
            new_job = self.jobs.pop(0)
            self.is_running_job = True
            prefix = f"mapreduce-shared-job{new_job['job_id']:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                LOGGER.info("Created tmpdir %s", tmpdir)
                # FIXME: Change this loop so that it runs either until shutdown 
                # or when the job is completed.
                # while self.working:

            LOGGER.info("Cleaned up tmpdir %s", tmpdir)
            self.is_running_job = False


    def finished(self, message_dict):
        # TODO: IMPLEMENT THIS
        pass


    def heartbeat(self, message_dict):
        # TODO: IMPLEMENT THIS
        pass


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
