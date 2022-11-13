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
from collections import defaultdict


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
        self.cur_job_message = None

        self.workers = {}
        self.working = True
        self.jobs = [] # job queue, add by append, remove by self.jobs.pop(0)
        self.num_jobs = 0 # assign job id

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

        thread3 = threading.Thread(target=self.run_job,)
        threads.append(thread3)
        thread3.start()
        # Create a new TCP socket on the given port and call the listen() function. 
        # Note: only one listen() thread should remain open for the whole lifetime of the Manager.
        self.create_TCP(self, None)

        thread1.join()
        thread2.join()
        thread3.join()

    def handler(self, message_dict):
        if message_dict["message_type"] == "shutdown":
            self.shutdown(message_dict)
            with self.lock:
                self.working = False
        elif message_dict["message_type"] == "register":
            self.register(message_dict)
        elif message_dict["message_type"] == "new_manager_job":
            self.new_manager_job(message_dict)
        elif message_dict["message_type"] == "finished":
            self.finished(message_dict)
        elif message_dict["message_type"] == "heartbeat":
            self.heartbeat(message_dict)


    def listen_worker_heartbeat(self, host, port):
        # Listen for UDP heartbeat messages from the workers
        # Create an INET, DGRAM socket, this is UDP
        # with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        #     # Bind the UDP socket to the server
        #     sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #     sock.bind((host, port))
        #     sock.settimeout(1)

        #     # No sock.listen() since UDP doesn't establish connections like TCP

        #     # TODO: IMPLEMENT THIS
        #     # Receive incoming UDP messages
        #     while self.working:
        #         try:
        #             message_bytes = sock.recv(4096)
        #         except socket.timeout:
        #             continue
        #         message_str = message_bytes.decode("utf-8")
        #         message_dict = json.loads(message_str)
        #         print(message_dict)
        pass


    def fault_tolerance(self, host, port):
        # TODO: IMPLEMENT THIS
        # Listen for UDP heartbeat messages from the workers
        # Create an INET, DGRAM socket, this is UDP
        # with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        #     # Bind the UDP socket to the server
        #     sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #     sock.bind((host, port))
        #     sock.settimeout(1)

        #     # No sock.listen() since UDP doesn't establish connections like TCP

        #     # TODO: IMPLEMENT THIS
        #     # Receive incoming UDP messages
        #     while self.working:
        #         try:
        #             message_bytes = sock.recv(4096)
        #         except socket.timeout:
        #             continue
        #         message_str = message_bytes.decode("utf-8")
        #         message_dict = json.loads(message_str)
        #         print(message_dict)
        pass


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
        self.cur_job_message = message_dict
        message_dict["job_id"] = self.num_jobs
        self.num_jobs += 1
        # Add the job to a queue.
        self.jobs.append(message_dict)
        # Delete the output directory if it exists. Create the output directory.
        output_directory_path = Path(message_dict["output_directory"])
        if output_directory_path.exists():
            output_directory_path.rmdir()
        output_directory_path.mkdir()
        # if self.jobs and self.is_running_job is False:
        #     self.run_job(message_dict)


    def input_partitioning(self, message_dict, tmpdir):
        # p = Path(directory).glob('**/*')
        # files = [x for x in p if x.is_file()]
        input_dir_path = Path(message_dict["input_directory"])
        files = list(input_dir_path.glob('**/*')).sort()
        partitioned_files = defaultdict(list)
        cur_task_id = 0
        for file in files:
            partitioned_files[cur_task_id].append(file)
            cur_task_id = (cur_task_id + 1) % message_dict["num_mappers"]
        task_id = 0
        for host, port in self.workers.keys():
            if self.workers[(host, port)] == "ready":
                new_message = {
                    "message_type": "new_map_task",
                    "task_id": task_id,
                    "input_paths": partitioned_files[task_id],
                    "executable": message_dict["mapper_executable"],
                    "output_directory": tmpdir,
                    "num_partitions": message_dict["num_reducers"],
                    "worker_host": host,
                    "worker_port": port,
                }
                mapreduce.utils.send_TCP_message(host, port, new_message)
                task_id += 1
                self.workers[(host, port)] = "busy"
                if task_id > len(partitioned_files.keys()):
                    break


    def run_job(self):
        while self.working and self.jobs:
            time.sleep(0.1)
            new_job = self.jobs.pop(0)
            prefix = f"mapreduce-shared-job{new_job['job_id']:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                LOGGER.info("Created tmpdir %s", tmpdir)
                # FIXME: Change this loop so that it runs either until shutdown 
                # or when the job is completed.
                self.input_partitioning(self.cur_job_message, tmpdir)
                # while self.working:
                #     time.sleep(0.1)
            LOGGER.info("Cleaned up tmpdir %s", tmpdir)


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
