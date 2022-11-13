"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import mapreduce.utils
import threading

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

        working =  True

        info = {
            "message_type": "register_ack",
            "worker_host": host,
            "worker_port": port,
        }
        mapreduce.utils.send_TCP_message(host, port, info)

        # Create a new thread, which will listen for UDP heartbeat messages from the Workers.
        threads = []
        thread1 = threading.Thread(target=self.send_heartbeat, args=(port,))
        threads.append(thread1)
        thread1.start()

        # Create a new TCP socket on the given port and call the listen() function. 
        # Note: only one listen() thread should remain open for the whole lifetime of the Manager.
        
        message_dict = mapreduce.utils.create_TCP(manager_host, manager_port)

        while working:
            message_dict = mapreduce.utils.create_TCP(manager_host, manager_port)
            if message_dict["message_type"] == "shutdown":
                working = False
            elif message_dict["message_type"] == "register_ack":
                self.register(message_dict)
            elif message_dict["message_type"] == "new_map_task":
                self.mapping(message_dict)
            elif message_dict["message_type"] == "new_reduce_task":
                self.reducing(message_dict)

        thread1.join()

    def send_heartbeat(self):
        ## TODO ##
        return

    def register(self):
        ## TODO ##
        return

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
