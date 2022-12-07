from multiprocessing import Process
from time import time, sleep
from kazoo.client import KazooClient

class Philosopher(Process):

    def __init__(self, root: str, forks_path: str, 
                 id: int, set_count: int, 
                 life_time: float, dining_time: float):
        super().__init__()
        
        self.root = root
        self.forks_path = forks_path
        
        self.id = id
        self.dining_time = dining_time
        self.life_time = life_time
        
        self.count = 0
        self.state = "thinking"
        
        self.left_fork_id = id
        self.right_fork_id = (id + 1) % set_count
        
        self.kz = None
    
    def change_state(self, state: str) -> None:
        self.state = state
        print(self)
    
    def join_table(self) -> None:
        self.kz.start()
    
    def leave_table(self) -> None:
        self.kz.stop()
        self.kz.close()
    
    
    def run(self) -> None: 
        
        self.kz = KazooClient()
        
        self.join_table()

        table_lock = self.kz.Lock(self.forks_path, self.id)
        left_fork = self.kz.Lock(f"{self.forks_path}/{self.left_fork_id}", self.id)
        right_fork = self.kz.Lock(f"{self.forks_path}/{self.right_fork_id}", self.id)

        start = time()
        
        while time() - start < self.life_time:
            
            with table_lock:
                
                if not (left_fork.contenders() or right_fork.contenders()):
                    left_fork.acquire()
                    right_fork.acquire()
                    print(self, "acquiered forks.")

            if left_fork.is_acquired and right_fork.is_acquired:
                self.change_state("eating")
                self.count += 1
                
                sleep(self.dining_time)
               
                left_fork.release()
                right_fork.release() 

                self.change_state("thinking")
                
                print(self, "leave forks.")
            
        print(f'Philosopher with id={self.id} eat counter={self.count}')
        
        self.leave_table()
    
    
    def __repr__(self) -> str:
        return f"Philosopher(id={self.id}, state={self.state})"
    
    