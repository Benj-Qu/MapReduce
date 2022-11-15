"""MapReduce framework Worker node."""
import os
import logging
import time
import threading
import hashlib
import subprocess
import tempfile
import shutil
import heapq
from pathlib import Path
from contextlib import ExitStack
import click
import mapreduce.utils


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """Worker class."""
    get_working = mapreduce.utils.get_working
    create_tcp = mapreduce.utils.create_tcp


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
        self.lock = threading.Lock()
        self.vars = {
                    "host": host, "port": port,
                    "manager_host": manager_host,
                    "manager_port": manager_port,
                    "working": True
                    }
        # self.vars['host'] = host
        # self.vars['port'] = port
        # self.vars['manager_host'] = manager_host
        # self.vars['manager_port'] = manager_port
        self.registered = False
        # self.vars["working"] = True
        self.thread = threading.Thread(target=self.heartbeat)
        register_message = {
            "message_type": "register",
            "worker_host": self.vars['host'],
            "worker_port": self.vars['port'],
        }
        self.create_tcp(register_message)
        if self.registered:
            self.thread.join()


    def tcp_handler(self, message_dict):
        """TCP handler for worker class."""
        if message_dict["message_type"] == "shutdown":
            self.vars["working"] = False
        elif message_dict["message_type"] == "register_ack":
            self.registered = True
            self.thread.start()
        elif message_dict["message_type"] == "new_map_task":
            self.mapping(message_dict)
        elif message_dict["message_type"] == "new_reduce_task":
            self.reducing(message_dict)


    def heartbeat(self):
        """Send heartbeat thread function for worker class."""
        while self.vars["working"]:
            heartbeat_msg = {
                "message_type": "heartbeat",
                "worker_host": self.vars['host'],
                "worker_port": self.vars['port'],
            }
            mapreduce.utils.send_udp_message(
                                            self.vars['manager_host'],
                                            self.vars['manager_port'],
                                            heartbeat_msg
                                            )
            time.sleep(2)


    def mapping(self, message_dict):
        """Mapping for worker class."""
        # prefix = f"mapreduce-local-task{message_dict['task_id']:05d}-"
        with tempfile.TemporaryDirectory(prefix
=f"mapreduce-local-task{message_dict['task_id']:05d}-")as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            for input_path in message_dict["input_paths"]:
                with open(input_path, encoding='utf-8') as infile:
                    with subprocess.Popen(
                        [message_dict["executable"]],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        for line in map_process.stdout:
                            # key = line.split("\t")[0]
                            # hexdigest = hashlib.md5(line.split("\t")[0].encode("utf-8")).hexdigest()
                            # keyhash = int(hashlib.md5(line.split("\t")[0].encode("utf-8")).hexdigest(), base=16)
                            partition = int(hashlib.md5(line.split("\t")[0].encode("utf-8")).hexdigest(), base=16) % message_dict['num_partitions']
                            # correct partiton output file
                            filename = Path(tmpdir) / f"maptask{message_dict['task_id']:05d}-part{partition:05d}"
                            with open(filename, 'a+', encoding='utf-8') as outfile:
                                outfile.write(line)
            # Sort each file in the temporary directory
            # output_dir = Path(message_dict["output_directory"])
            output_files = [
                            Path(message_dict["output_directory"]) / f"maptask{message_dict['task_id']:05d}-part{partition:05d}"
                            for partition in range(message_dict['num_partitions'])
                            ]
            with ExitStack() as stack:
                output_files = [
                                stack.enter_context(open(file, "a", encoding="utf8"))
                                for file in output_files
                                ]
                for partition in range(message_dict['num_partitions']):
                    filename = Path(tmpdir) / f"maptask{message_dict['task_id']:05d}-part{partition:05d}"
                    with open(filename, encoding='utf-8') as input_file:
                        lines = sorted(input_file.readlines())
                    for line in lines:
                        output_files[partition].write(line)
            finish_msg = {
                "message_type": "finished",
                "task_id": message_dict['task_id'],
                "worker_host": self.vars['host'],
                "worker_port": self.vars['port'],
            }
            mapreduce.utils.send_tcp_message(self.vars['manager_host'], self.vars['manager_port'], finish_msg)
        LOGGER.info("Cleaned up tmpdir %s", tmpdir)


    def reducing(self, message_dict):
        """Reducing for worker class."""
        prefix = f"mapreduce-local-task{message_dict['task_id']:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            with ExitStack() as stack:
                files = [stack.enter_context(open(fname, "r+", encoding="utf8"))
                        for fname in message_dict['input_paths']]
                instream = heapq.merge(*files)

                tmpdir_path = Path(tmpdir)
                outfile_path =  tmpdir_path / f"part-{message_dict['task_id']:05d}"
                with open(outfile_path, "a+", encoding='utf-8') as outfile:
                    with subprocess.Popen(
                        [message_dict['executable']],
                        text=True,
                        stdin=subprocess.PIPE,
                        stdout=outfile,
                    ) as reduce_process:
                        # Pipe input to reduce_process
                        for line in instream:
                            reduce_process.stdin.write(line)

            shutil.copytree(tmpdir_path, message_dict['output_directory'], dirs_exist_ok=True)

            message = {
                "message_type": "finished",
                "task_id": message_dict['task_id'],
                "worker_host": self.vars['host'],
                "worker_port": self.vars['port']
            }
            mapreduce.utils.send_tcp_message(self.vars['manager_host'], self.vars['manager_port'], message)


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
