"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import time
import threading
from pathlib import Path
from collections import defaultdict
from enum import Enum
import click
import mapreduce.utils


# Configure logging
LOGGER = logging.getLogger(__name__)

class Status(Enum):
    """Status for worker."""
    READY = 1
    BUSY = 2
    DEAD = 3

class WorkerInfo:
    """WorkerInfo class."""
    def __init__(self):
        self._birth = time.time()
        self._status = Status.READY
        self._taskid = -1

    @property
    def birth(self):
        """Last heartbeat time."""
        return self._birth

    @birth.setter
    def birth(self, value):
        """Set last heartbeat time."""
        self._birth = value

    @property
    def status(self):
        """Worker status."""
        return self._status

    @status.setter
    def status(self, value):
        """Set worker status."""
        self._status = value

    @property
    def taskid(self):
        """Worker taskid."""
        return self._taskid

    @taskid.setter
    def taskid(self, value):
        """Set worker taskid."""
        self._taskid = value


class Manager:
    """Represent a MapReduce framework Manager node."""

    get_working = mapreduce.utils.get_working
    create_tcp = mapreduce.utils.create_tcp
    create_udp = mapreduce.utils.create_udp

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        self.vars = {
                    "host": host, "port": port,
                    "tmpdir": None, "cur_job_message": {},
                    "workers": {}, "ready_workers": [],
                    "working": True, "jobs": [],
                    "num_jobs": 0, "mapping_task": True,
                    "tasks": [], "task_content": defaultdict(list),
                    "num_finished": 0
                    }
        # self.vars['tmpdir'] = None
        # self.vars["cur_job_message"] = None
        self.lock = threading.Lock()
        # self.vars["workers"] = {}
        # self.vars["ready_workers"] = []
        # self.vars["working"] = True
        # self.vars["jobs"] = [] # job queue, add by append, remove by self.vars["jobs"].pop(0)
        # self.vars["num_jobs"] = 0 # assign job id
        # self.vars["mapping_task"] = True
        # self.vars["tasks"] = [] # task queue, add by append, remove by self.vars["tasks"].pop(0)
        # self.vars["task_content"] = defaultdict(list) # task content dictionary
        # self.vars["num_finished"] = 0
        # Create a new thread, which will listen for udp heartbeat messages from the Workers.
        thread1 = threading.Thread(target=self.create_udp, args=(host, port,))
        thread1.start()
        # Create any additional threads or setup you think you may need.
        # Another thread for fault tolerance could be helpful.
        thread2 = threading.Thread(target=self.fault_tolerance)
        thread2.start()
        thread3 = threading.Thread(target=self.run_job)
        thread3.start()
        # Create a new tcp socket on the given port and call the listen() function.
        # Note: only one listen() thread should remain open for the whole lifetime of the Manager.
        self.create_tcp(None)
        thread1.join()
        thread2.join()
        thread3.join()


    def tcp_handler(self, message_dict):
        """TCP handler for manager class."""
        # LOGGER.info(message_dict["message_type"])
        if message_dict["message_type"] == "shutdown":
            self.shutdown(message_dict)
            with self.lock:
                self.vars["working"] = False
        elif message_dict["message_type"] == "register":
            self.register(message_dict)
        elif message_dict["message_type"] == "new_manager_job":
            self.new_manager_job(message_dict)
        elif message_dict["message_type"] == "finished":
            self.finished(message_dict)
        else:
            print("Undedfined message!")


    def udp_handler(self, msg_dict):
        """UDP handler for manager class."""
        host = msg_dict["worker_host"]
        port = msg_dict["worker_port"]
        worker = (host, port)
        with self.lock:
            if worker in self.vars["workers"].keys():
                self.vars["workers"][worker].birth = time.time()


    def fault_tolerance(self):
        """Fault tolerance thread function."""
        while self.get_working():
            with self.lock:
                for worker in self.vars["workers"].keys():
                    if (self.vars["workers"][worker].status != Status.DEAD and
                        time.time() - self.vars["workers"][worker].birth > 10):
                        self.deal_dead_workers(worker)
            time.sleep(0.5)


    def tasks_isempty(self):
        """Get whether tasks are empty."""
        with self.lock:
            return len(self.vars["tasks"]) == 0


    def jobs_isempty(self):
        """Get whether jobs are empty."""
        with self.lock:
            return len(self.vars["jobs"]) == 0


    def shutdown(self, message_dict):
        """Handle shutdown messages."""
        # Forward this message to all of the living Workers
        # that have registered with it
        with self.lock:
            for host, port in self.vars["workers"].keys():
                if self.vars["workers"][(host, port)].status != Status.DEAD:
                    send_tcp_success = mapreduce.utils.send_tcp_message(host, port, message_dict)
                    if not send_tcp_success:
                        self.deal_dead_workers((host, port))


    def register(self, message_dict):
        """Handle register messages."""
        # Register a worker
        host = message_dict["worker_host"]
        port = message_dict["worker_port"]
        worker = (host, port)
        with self.lock:
            if (worker in self.vars["workers"].keys() and
                self.vars["workers"][worker].status == Status.BUSY):
                self.vars["workers"][worker].status = Status.DEAD
                self.assign_task(self.vars["workers"][worker].taskid)
        with self.lock:
            self.vars["workers"][worker] = WorkerInfo()
            self.vars["ready_workers"].append(worker)
        message_dict["message_type"] = "register_ack"
        send_tcp_success = mapreduce.utils.send_tcp_message(host, port, message_dict)
        if not send_tcp_success:
            with self.lock:
                self.deal_dead_workers(worker)
        else:
            with self.lock:
                if self.vars["tasks"]:
                    taskid = self.vars["tasks"].pop(0)
                    self.assign_task(taskid)


    def new_manager_job(self, message_dict):
        """If receive a new manager job."""
        # Assign a job_id which starts from zero and increments.
        message_dict["job_id"] = self.vars["num_jobs"]
        with self.lock:
            self.vars["cur_job_message"] = message_dict
            self.vars["num_jobs"] += 1
            # Add the job to a queue.
            self.vars["jobs"].append(message_dict)
        # Delete the output directory if it exists. Create the output directory.
        output_directory_path = Path(message_dict["output_directory"])
        if output_directory_path.exists():
            output_directory_path.rmdir()
        output_directory_path.mkdir()


    def assign_task(self, taskid):
        """Assign task with taskid to ready worker,
        or add to tasks list
        NO NEED TO LOCK! LOCK BEFORE THE FUNCTION!"""
        if self.vars["ready_workers"]:
            # Assign the tasks with taskid to ready worker
            worker_host, worker_port = self.vars["ready_workers"].pop(0)
            worker = (worker_host, worker_port)
            self.vars["workers"][worker].status = Status.BUSY
            self.vars["workers"][worker].taskid = taskid
            if self.vars["mapping_task"]:
                new_message = {
                    "message_type": "new_map_task",
                    "task_id": taskid,
                    "input_paths": self.vars["task_content"][taskid],
                    "executable": self.vars["cur_job_message"]["mapper_executable"],
                    "output_directory": self.vars['tmpdir'],
                    "num_partitions": self.vars["cur_job_message"]["num_reducers"],
                    "worker_host": worker_host,
                    "worker_port": worker_port,
                }
            else:
                new_message = {
                    "message_type": "new_reduce_task",
                    "task_id": taskid,
                    "executable": self.vars["cur_job_message"]["reducer_executable"],
                    "input_paths": self.vars["task_content"][taskid],
                    "output_directory": self.vars["cur_job_message"]["output_directory"],
                    "worker_host": worker_host,
                    "worker_port": worker_port,
                }
            send_tcp_success = mapreduce.utils.send_tcp_message(worker_host,
                                                                worker_port,
                                                                new_message)
            if not send_tcp_success:
                with self.lock:
                    self.deal_dead_workers((worker_host, worker_port))
        else:
            # No ready workers, add this task to tasks list
            if taskid not in self.vars["tasks"]:
                self.vars["tasks"].append(taskid)


    def deal_dead_workers(self, worker):
        """Deal with dead workers."""
        if self.vars["workers"][worker].status == Status.BUSY:
            self.vars["workers"][worker].status = Status.DEAD
            self.assign_task(self.vars["workers"][worker].taskid)
        elif self.vars["workers"][worker].status == Status.READY:
            self.vars["workers"][worker].status = Status.DEAD
            self.vars["ready_workers"].remove(worker)


    def input_partitioning(self):
        """Input partitioning."""
        self.vars["mapping_task"] = True
        input_dir_path = Path(self.vars["cur_job_message"]["input_directory"])
        files = list(input_dir_path.glob('**/*'))
        for idx, file in enumerate(files):
            files[idx] = str(file)
        files.sort()
        self.vars["task_content"] = defaultdict(list)
        cur_task_id = 0
        num_mappers = self.vars["cur_job_message"]["num_mappers"]
        for file in files:
            self.vars["task_content"][cur_task_id].append(file)
            cur_task_id = (cur_task_id + 1) % num_mappers
        self.vars["tasks"] = list(range(num_mappers))
        # Mapping
        self.vars["mapping_task"] = True
        self.vars["num_finished"] = 0
        while self.get_working():
            time.sleep(0.1)
            with self.lock:
                if self.vars["num_finished"] == num_mappers:
                    break
                while self.vars["ready_workers"] and self.vars["tasks"]:
                    taskid = self.vars["tasks"].pop(0)
                    self.assign_task(taskid)


    def run_job(self):
        """Run job."""
        while self.get_working():
            time.sleep(0.1)
            if not self.jobs_isempty():
                with self.lock:
                    job = self.vars["jobs"].pop(0)
                prefix = f"mapreduce-shared-job{job['job_id']:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    self.vars['tmpdir'] = tmpdir
                    self.input_partitioning()
                    self.run_reducing()
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)


    def run_reducing(self):
        """Run reducing job."""
        self.vars["mapping_task"] = False
        tmp_dir_path = Path(self.vars['tmpdir'])
        files = list(tmp_dir_path.glob('**/*'))
        for idx, file in enumerate(files):
            files[idx] = str(file)
        files.sort()
        self.vars["task_content"] = defaultdict(list)
        for file in files:
            partition = int(file[-5:])
            self.vars["task_content"][partition].append(file)
        self.vars["tasks"] = list(range(self.vars["cur_job_message"]["num_reducers"]))
        # Reducing
        self.vars["num_finished"] = 0
        while self.get_working():
            time.sleep(0.1)
            with self.lock:
                if self.vars["num_finished"] == self.vars["cur_job_message"]["num_reducers"]:
                    break
                while self.vars["ready_workers"] and self.vars["tasks"]:
                    taskid = self.vars["tasks"].pop(0)
                    self.assign_task(taskid)


    def finished(self, message_dict):
        """Handle finished messages."""
        worker_host = message_dict["worker_host"]
        worker_port = message_dict["worker_port"]
        worker = (worker_host, worker_port)
        with self.lock:
            if worker in self.vars["workers"].keys():
                self.vars["workers"][worker].status = Status.READY
                self.vars["workers"][worker].taskid = -1
                self.vars["ready_workers"].append(worker)
                self.vars["num_finished"] += 1


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
