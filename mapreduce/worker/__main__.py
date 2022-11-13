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
        self.isWorking = True

        self.thread = threading.Thread(target=self.heartbeat)

        register_message = {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port,   
        }

        mapreduce.utils.create_TCP(self.host, self.port, self.handler, self.working ,self.manager_host, self.manager_port, register_message)

        if self.registered:
            self.thread.join()

    def working(self):
        return self.isWorking

    def handler(self, message_dict):
        if message_dict["message_type"] == "shutdown":
            self.working = False
        elif message_dict["message_type"] == "register_ack":
            self.registered = True
            self.thread.start()
        elif message_dict["message_type"] == "new_map_task":
            self.mapping(message_dict)
        elif message_dict["message_type"] == "new_reduce_task":
            self.reducing(message_dict)

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
