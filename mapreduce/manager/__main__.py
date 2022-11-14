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
from enum import Enum


# Configure logging
LOGGER = logging.getLogger(__name__)

class Status(Enum):
    READY = 1
    BUSY = 2
    DEAD = 3

class WorkerInfo:
    def __init__(self):
        self._birth = time.time()
        self._status = Status.READY
        self._taskid = -1

    @property
    def birth(self):
        return self._birth
    
    @birth.setter
    def birth(self, value):
        self._birth = value
    
    @property
    def status(self):
        return self._status
    
    @status.setter
    def status(self, value):
        self._status = value
        
    @property
    def taskid(self):
        return self._taskid
    
    @taskid.setter
    def taskid(self, value):
        self._taskid = value


class Manager:
    """Represent a MapReduce framework Manager node."""

    get_working = mapreduce.utils.get_working
    create_TCP = mapreduce.utils.create_TCP
    create_UDP = mapreduce.utils.create_UDP

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        self.host = host
        self.port = port
        self.tmpdir = None
        self.cur_job_message = None
        self.lock = threading.Lock()

        self.workers = {}
        self.ready_workers = []
        self.working = True
        self.jobs = [] # job queue, add by append, remove by self.jobs.pop(0)
        self.num_jobs = 0 # assign job id
        self.mapping_task = True
        self.tasks = [] # task queue, add by append, remove by self.tasks.pop(0)
        self.task_content = defaultdict(list) # task content dictionary
        self.num_finished = 0

        # Create a new thread, which will listen for UDP heartbeat messages from the Workers.
        threads = []
        thread1 = threading.Thread(target=self.create_UDP, args=(host, port,))
        threads.append(thread1)
        thread1.start()
        # Create any additional threads or setup you think you may need. 
        # Another thread for fault tolerance could be helpful.
        thread2 = threading.Thread(target=self.fault_tolerance)
        threads.append(thread2)
        thread2.start()

        thread3 = threading.Thread(target=self.run_job)
        threads.append(thread3)
        thread3.start()
        # Create a new TCP socket on the given port and call the listen() function. 
        # Note: only one listen() thread should remain open for the whole lifetime of the Manager.
        self.create_TCP(None)

        thread1.join()
        thread2.join()
        thread3.join()


    def TCP_handler(self, message_dict):
        # LOGGER.info(message_dict["message_type"])
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
        else:
            print("Undedfined message!")


    def UDP_handler(self, msg_dict):
        host = msg_dict["worker_host"]
        port = msg_dict["worker_port"]
        with self.lock:
            self.workers[host, port].birth = time.time()


    def fault_tolerance(self):
        while self.get_working():
            with self.lock:
                for worker in self.workers.keys():
                    if (self.workers[worker].status != Status.DEAD and 
                        time.time() - self.workers[worker].birth > 10):
                        if self.workers[worker].status == Status.BUSY:
                            self.assign_task(self.workers[worker].taskid)
                        elif self.workers[worker].status == Status.READY:
                            self.ready_workers.remove(worker)
                        self.workers[worker].status = Status.DEAD
            time.sleep(0.5)


    def tasks_isempty(self):
        with self.lock:
            return (len(self.tasks) == 0)


    def jobs_isempty(self):
        with self.lock:
            return (len(self.jobs) == 0)


    def shutdown(self, message_dict):
        # Forward this message to all of the living Workers
        # that have registered with it
        for host, port in self.workers.keys():
            mapreduce.utils.send_TCP_message(host, port, message_dict)


    def register(self, message_dict):
        # Register a worker
        host = message_dict["worker_host"]
        port = message_dict["worker_port"]
        worker = (host, port)
        with self.lock:
            if (worker in self.workers.keys() and 
                self.workers[worker].status == Status.BUSY):
                self.workers[worker].status = Status.DEAD
                self.assign_task(self.workers[worker].taskid)
        with self.lock:
            self.workers[worker] = WorkerInfo()
            self.ready_workers.append(worker)
        message_dict["message_type"] = "register_ack"
        mapreduce.utils.send_TCP_message(host, port, message_dict)
        with self.lock:
            if self.tasks:
                taskid = self.tasks.pop(0)
                self.assign_task(taskid)


    def new_manager_job(self, message_dict):
        # Assign a job_id which starts from zero and increments.
        message_dict["job_id"] = self.num_jobs
        with self.lock:
            self.cur_job_message = message_dict
            self.num_jobs += 1
            # Add the job to a queue.
            self.jobs.append(message_dict)
        # Delete the output directory if it exists. Create the output directory.
        output_directory_path = Path(message_dict["output_directory"])
        if output_directory_path.exists():
            output_directory_path.rmdir()
        output_directory_path.mkdir()


    def assign_task(self, taskid):
        # Assign task with taskid to ready worker, or add to tasks list
        # NO NEED TO LOCK! LOCK BEFORE THE FUNCTION!
        if self.ready_workers:
            # Assign the taks with taskid to ready worker
            worker_host, worker_port = self.ready_workers.pop(0)
            if self.mapping_task:
                new_message = {
                    "message_type": "new_map_task",
                    "task_id": taskid,
                    "input_paths": self.task_content[taskid],
                    "executable": self.cur_job_message["mapper_executable"],
                    "output_directory": self.tmpdir,
                    "num_partitions": self.cur_job_message["num_reducers"],
                    "worker_host": worker_host,
                    "worker_port": worker_port,
                }
            else:
                new_message = {
                    "message_type": "new_reduce_task",
                    "task_id": taskid,
                    "executable": self.cur_job_message["reducer_executable"],
                    "input_paths": self.task_content[taskid],
                    "output_directory": self.cur_job_message["output_directory"],
                    "worker_host": worker_host,
                    "worker_port": worker_port,
                }
            mapreduce.utils.send_TCP_message(worker_host, worker_port, new_message)
            # If successfully assign the task
            self.tasks.remove(taskid)
        else:
            # No ready workers, add this task to tasks list
            if taskid not in self.tasks:
                self.tasks.append(taskid)


    def input_partitioning(self):
        self.mapping_task = True
        input_dir_path = Path(self.cur_job_message["input_directory"])
        files = list(input_dir_path.glob('**/*'))
        for idx, file in enumerate(files):
            files[idx] = str(file)
        files.sort()
        self.task_content = defaultdict(list)
        cur_task_id = 0
        num_mappers = self.cur_job_message["num_mappers"]
        for file in files:
            self.task_content[cur_task_id].append(file)
            cur_task_id = (cur_task_id + 1) % num_mappers
        self.tasks = list(range(num_mappers))
        # Mapping
        self.mapping_task = True
        self.num_finished = 0
        while self.get_working():
            time.sleep(0.1)
            with self.lock:
                if self.num_finished == num_mappers:
                    break
                for taskid in self.tasks:
                    self.assign_task(taskid)


    def run_job(self):
        while self.get_working():
            time.sleep(0.1)
            if not self.jobs_isempty():
                with self.lock:
                    job = self.jobs.pop(0)
                prefix = f"mapreduce-shared-job{job['job_id']:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    # FIXME: Change this loop so that it runs either until shutdown 
                    # or when the job is completed.
                    # while self.get_working():
                    #     time.sleep(0.1)
                    self.tmpdir = tmpdir
                    self.input_partitioning()
                    self.run_reducing()
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)


    def run_reducing(self):
        self.mapping_task = False
        tmp_dir_path = Path(self.tmpdir)
        files = list(tmp_dir_path.glob('**/*'))
        for idx, file in enumerate(files):
            files[idx] = str(file)
        self.task_content = defaultdict(list)
        for file in files:
            partition = int(file[-5:])
            self.task_content[partition].append(file)
        self.tasks = list(range(self.cur_job_message["num_reducers"]))
        # Mapping
        self.num_finished = 0
        while self.get_working():
            time.sleep(0.1)
            if self.num_finished == self.cur_job_message["num_reducers"]:
                break
            for taskid in self.tasks:
                self.assign_task(taskid)


    def finished(self, message_dict):
        worker_host = message_dict["worker_host"]
        worker_port = message_dict["worker_port"]
        worker = (worker_host, worker_port)
        with self.lock:
            self.workers[worker].status = Status.READY
            self.ready_workers.append(worker)
            self.num_finished += 1


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
