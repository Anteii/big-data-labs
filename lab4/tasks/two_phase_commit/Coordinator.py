import threading
from kazoo.client import KazooClient
from typing import List
 

class Coordinator:

    COMMIT = b"commit"
    ABORT = b"abort"
    
    
    def __init__(self, root: str, n_clients: int, 
                 timeout: int, strict_mode=False):
        
        self.strict_mode = strict_mode
        self.timeout = timeout
        self.n_clients = n_clients
        self.root = root
        self.transaction_path = root + "/tx"
        self.timer = None

        # Create base node
        self.kz = KazooClient()
        self.kz.start()
        if self.kz.exists(self.root):
            self.kz.delete(self.root, recursive=True)
        self.kz.create(self.root)
        
        # Create transaction node
        self.kz.create(self.transaction_path)
        #print(self.transaction_path)
        # Set WATCH on transaction node
        @self.kz.ChildrenWatch(self.transaction_path)
        def watch_clients(clients):

            # cancel timer if something changed
            if self.timer is not None:
                self.timer.cancel()

            if len(clients) != 0:
                self.init_timer()

                if len(clients) < self.n_clients:
                    print('Waiting for others...', clients)
                elif len(clients) == self.n_clients:
                    self.timer.cancel()
                    decision = self.decide()
                    # Changes the value of ephimeral nodes for each client
                    self.send_decision(decision)


    def init_timer(self) -> None:
        self.timer = threading.Timer(self.timeout, lambda: self._decide())
        self.timer.daemon = True
        self.timer.start()
    
    def collect_opinions(self) -> List[bytes]:
        print("Collecting votes...")
        
        clients = self.kz.get_children(self.transaction_path)
        votes = [self.kz.get(f'{self.transaction_path}/{client}')[0] for client in clients]
        
        return votes

    def decide(self) -> bytes:
        print("Making decision...")
        
        votes = self.collect_opinions()
        
        aborts_count = votes.count(self.ABORT)
        commits_count = votes.count(self.COMMIT)

        decision = self.COMMIT if commits_count > aborts_count else self.ABORT

        return decision
    
    def send_decision(self, decision: bytes) -> None:
        print("Sending decision...")
        
        clients = self.kz.get_children(self.transaction_path)
        
        for client in clients:
            self.kz.set(f'{self.transaction_path}/{client}', decision)
