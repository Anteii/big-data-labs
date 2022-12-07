from kazoo.client import KazooClient
from time import sleep
from Philosopher import Philosopher


root = '/table'
forks_path = root + "/" + "forks"
seat_count = 5


def create_node(kz: KazooClient, path: str) -> None:
    if kz.exists(path):
        kz.delete(path, recursive=True)
    kz.create(path)


if __name__ == "__main__":
    
    kz = KazooClient()
    kz.start()
    
    if kz.exists(root):
        kz.delete(root, recursive=True)
    
    kz.create(root)
    kz.create(forks_path)
    
    for i in range(seat_count):
        kz.create(forks_path + "/" + str(i))

    print("Dinner is starting!")
    
    philosophers = []
    
    for i in range(0, seat_count):
        p = Philosopher(root, forks_path, 
                        i, seat_count,
                        20, 1)
        philosophers.append(p)
    
    for philosopher in philosophers:
        philosopher.start()
    
    while philosophers:
        for philosopher in philosophers:
            if not philosopher.is_alive():
                philosophers.remove(philosopher)
        sleep(2)
    
    print("Dinner has come to its end!")
    
    kz.stop()
    kz.close()