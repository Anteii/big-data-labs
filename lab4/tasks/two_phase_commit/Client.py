import random
from time import time
from multiprocessing import Process
from time import sleep
from kazoo.client import KazooClient

class Client(Process):
    COMMIT = b"commit"
    ABORT = b"abort"
    COMMITED = b"commited"
    ABORTED = b"aborted"
    
    def __init__(self, root: str, id: int, ttl: int = 4):
        super().__init__()
        self.root = root
        self.transaction_path = root + "/tx"
        self.id = id
        self.node_path = self.transaction_path + "/" + str(id)
        self.ttl = ttl
        
        self.kz = None

    # @Override
    def run(self):
        
        random.seed(int(time()) + self.id * 100)
        
        self.kz = KazooClient()
        self.kz.start()

        
        # whait for transaction node to be created
        while not self.kz.exists(self.transaction_path):
            sleep(1)
        
        # Make a decision
        value = self.COMMIT if random.random() > 0.5 else self.ABORT
        print(f"Client {self.id} decision is {value.decode()}")
        
        # Create node with decision
        #print(self.node_path)
        self.kz.create(self.node_path, value, ephemeral=True)

        # Set watch on a decision node 
        @self.kz.DataWatch(self.node_path)
        def watch_myself(data, stat):
            if stat.version != 0:
                self.value = stat
                # Commiting / aborting
                if self.value == self.ABORT:
                    print(f"Client {self.id} aborting transaction")
                    self.kz.set(self.node_path, self.ABORTED)
                elif self.value == self.COMMIT:
                    print(f"Client {self.id} commiting transaction")
                    self.kz.set(self.node_path, self.COMMITED)

        sleep(self.ttl)

        self.kz.stop()
        self.kz.close()