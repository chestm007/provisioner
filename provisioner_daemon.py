import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor

import psycopg2.extras
import sys

import time

from graph import ExecutionGraph, AWSGraphBuilder, ExecutionException
from providers import BaseProvider
from tasks import TaskState, TaskAction

log = logging.getLogger(__name__)

log.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)


class Worker(threading.Thread):

    def __init__(self, result_queue, connection):
        super(Worker, self).__init__()
        self._stop_event = threading.Event()
        self.queue = result_queue
        self.connection = connection

    def run(self):
        while not self.stopped():
            try:
                result = self.queue.get(timeout=1).result()
                if result.state == TaskState.PROVISIONED:
                    log.info("Completed task: {}".format(result))
            except queue.Empty:
                pass
            except ExecutionException as e:
                log.error("{} - {}".format(e.task, e.exception))
            finally:
                self.connection.commit()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


class Provisioner(threading.Thread):
    def __init__(self, connection):
        super(Provisioner, self).__init__()
        self._stop_event = threading.Event()
        self.connection = connection
        self.clusters = {}
        self.result_queue = queue.Queue()
        self.pool = ThreadPoolExecutor(100)
        self.worker = Worker(self.result_queue, connection)

    def on_done(self, future):
        self.result_queue.put(future)

    def new_cursor(self):
        return self.connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

    def run(self):
        provider = BaseProvider()
        self.get_clusters_with_pending_nodes()
        self.worker.start()
        while not self.stopped():
            if not self.clusters:
                has_pending_nodes = self.get_clusters_with_pending_nodes()
                if not has_pending_nodes:
                    time.sleep(60)
            clusters = dict(self.clusters)
            for id, graph in clusters.items():
                print(graph.info())
                if graph.percent_complete() == 100:
                    self.clusters.pop(id)

                provisioning_tasks = graph.provisioning_tasks()
                deletion_tasks = graph.deletion_tasks()

                if provisioning_tasks:
                    for task in provisioning_tasks:
                        self.pool.submit(task, self.new_cursor(), provider, TaskAction.PROVISION).add_done_callback(self.on_done)
                elif deletion_tasks:
                    for task in deletion_tasks:
                        self.pool.submit(task, self.new_cursor(), TaskAction.DELETE).add_done_callback(self.on_done)

            try:
                time.sleep(1)
            except KeyboardInterrupt:
                self.stop()

    def get_clusters_with_pending_nodes(self):
        cursor = self.new_cursor()
        cursor.execute("""
            SELECT DISTINCT(cluster) 
            FROM node 
            WHERE state IN ('PENDING_PROVISION', 'PENDING_DELETION');
        """)
        clusters = list(c for c in cursor)
        builder = AWSGraphBuilder(self.connection)
        self.clusters.update({c: ExecutionGraph(builder.load(c)) for c in clusters})
        if clusters:
            return True

    def stop(self):
        self.worker.stop()
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()