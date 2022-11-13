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

        self.workers = {}
        self.working = True
        self.jobs = [] # job queue, add by append, remove by self.jobs.pop(0)
        self.num_jobs = 0 # assign job id

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
        while self.working:
            message_dict = mapreduce.utils.create_TCP(host, port)
            if message_dict["message_type"] == "shutdown":
                self.working = False
                self.shutdown(message_dict)
            elif message_dict["message_type"] == "register":
                self.register(message_dict)
            elif message_dict["message_type"] == "new_manager_job":
                self.new_manager_job(message_dict)
            elif message_dict["message_type"] == "finished":
                self.finished(message_dict)
            elif message_dict["message_type"] == "heartbeat":
                self.heartbeat(message_dict)

        thread1.join()
        thread2.join()

        # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # # TODO: you should remove this. This is just so the program doesn't
        # # exit immediately!
        # LOGGER.debug("IMPLEMENT ME!")
        # time.sleep(120)


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
