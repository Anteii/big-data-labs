from Coordinator import Coordinator
from Client import Client


root = "/2PC"
n_clients = 5
decision_timeout = 20

if __name__ == '__main__':
    coordinator = Coordinator(root=root, n_clients=n_clients, timeout=decision_timeout)
    
    clients = []
    
    for client_id in range(n_clients):
        client = Client(root, client_id)
        clients.append(client)
    
    for client in clients:
        client.start()

    while clients:
        clients = list(filter(lambda client: client.is_alive(), clients))
    