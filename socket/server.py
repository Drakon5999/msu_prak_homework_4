import socket
import threading
import multiprocessing
import os
import json
import sys
import logging

sys.path.append("..")
from libs import tools
from libs.constants import *

COMMANDS = {
    b"STAT": 0,
    b"ENTI": 1
}

logging.basicConfig(filename="sample.log", level=logging.INFO, filemode="w")
logger = logging.getLogger(__name__)


def process_request(conn, addr):
    logger.info("connected client:", addr)
    conn.settimeout(10)
    parser = tools.DataParser(COMMANDS)
    with conn:
        while not parser.is_data_ready:
            try:
                # logger.debug("Get data")
                data = conn.recv(1024)
                # logger.debug("Len: {}".format(len(data)))
                if not data:
                    break
                # logger.debug(data)
                parser.parse_data(data)
            except socket.timeout:
                if not parser.is_data_ready:
                    logger.debug("timeout")
                    return
            except tools.DataParser.BadCommandException as e:
                logger.debug("bad command")
                conn.sendall(b"bad command: " + e.command)
                return

        if not parser.is_data_ready:
            logger.debug("bad data size")
            conn.sendall(b"bad data size")
            if parser.data_size is not None:
                conn.sendall(b": " + int.to_bytes(parser.data_size, INT_SIZE, BYTE_ORDER))
            return

        try:
            logger.debug("json parse")
            twits = json.loads(parser.data_to_process.decode(ENCODING), encoding=ENCODING)
        except Exception:
            logger.debug("json parse failed")
            conn.sendall(b"Bad data. Expect utf-8 encoded json")
            return

        stat = []
        if parser.command == COMMANDS[b"STAT"]:
            stat = tools.TwitsStatCalculator(twits).get_full_report()
        elif parser.command == COMMANDS[b"ENTI"]:
            stat = tools.EntityExtractor(twits).extract_entities()

        logger.debug(stat)
        data_to_send = bytes(json.dumps(stat, ensure_ascii=False), ENCODING)
        conn.sendall(int.to_bytes(len(data_to_send), INT_SIZE, BYTE_ORDER))
        conn.sendall(data_to_send)


def worker(sock):
    try:
        while True:
            conn, addr = sock.accept()
            print("pid", os.getpid())
            th = threading.Thread(target=process_request, args=(conn, addr))
            th.start()
    except KeyboardInterrupt:
        sock.close()
        pass


with socket.socket() as sock:
    sock.bind(("", SERVER_PORT))
    sock.listen(5)

    workers_count = 3
    workers_list = [multiprocessing.Process(target=worker, args=(sock,))
                    for _ in range(workers_count)]
    try:
        for w in workers_list:
            w.start()

        for w in workers_list:
            w.join()
    except KeyboardInterrupt:
        sock.close()
        for w in workers_list:
            w.terminate()
