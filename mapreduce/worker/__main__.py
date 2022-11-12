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

        # Create a new thread, which will listen for UDP heartbeat messages from the Workers.
        threads = []
        thread1 = threading.Thread(target=self.send_heartbeat, args=(port,))
        threads.append(thread1)
        thread1.start()

        # Create a new TCP socket on the given port and call the listen() function. 
        # Note: only one listen() thread should remain open for the whole lifetime of the Manager.
        
        message_dict = mapreduce.utils.create_TCP(port)
        self.message_handler(message_dict)

        thread1.join()

        
        # Create a new TCP socket on the given port and call the listen() function.
        messgage_dict = mapreduce.utils.create_TCP(port)
        
        # # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register_ack",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # # TODO: you should remove this. This is just so the program doesn't
        # # exit immediately!
        # LOGGER.debug("IMPLEMENT ME!")
        # time.sleep(120)

    def send_heartbeat():
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
