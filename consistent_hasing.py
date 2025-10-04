from abc import ABC,abstractmethod
import hashlib
import bisect
import mmh3



class HashStrategy(ABC):
    def hash(self,key:str) ->int:
        pass


class MD5Hash(HashStrategy):
    def hash(self,key:str):
        return int(hashlib.md5(key.encode()).hexdigest(),16)
    

class SHA256Hash(HashStrategy):
    def hash(self,key:str)->int:
        return int(hashlib.sha256(key.encode()).hexdigest(),16)
    

class MurmurHash(HashStrategy):
    def hash(self,key:str) ->int:
        return mmh3.hash(key,signed=False)
    



class ConsistentHashRing:
    def __init__(self,hash_strategy:HashStrategy,replicas: int=100):


        self.hash_strategy=hash_strategy
        self.replicas =replicas
        self.ring=dict()
        self.sorted_keys=[]


    def _hash(self,key:str) ->int:
        return self.hash_strategy.hash(key)
    
    def add_node(self,node:str):
        for i in range(self.replicas):
            virtual_node_key=f"{node}#{i}"
            hash_value=self._hash(virtual_node_key)
            self.ring[hash_value]=node
            bisect.insort(self.sorted_keys,hash_value)

    def remove_node(self,node:str):
        for i in range(self.replicas):
            virtual_node_key=f"{node}#{i}"
            hash_value=self._hash(virtual_node_key)
            if hash_value in self.ring:
                del self.ring[hash_value]
                index=bisect.bisect_left(self.sorted_keys,hash_value)
                if index <len(self.sorted_keys):
                    self.sorted_keys.pop(index)

    def get_node(self,key:str)->str:
        if not self.ring:
            return None
        
        hash_value=self._hash(key)
        index=bisect.bisect(self.sorted_keys,hash_value) % len(self.sorted_keys) 
        responsible_hash=self.sorted_keys[index]
        return self.ring[responsible_hash]
    
    def list_nodes(self):
        return sorted(set(self.ring.values()))
    



if __name__ =="__main__":
    hash_strategy = MurmurHash()  # you can switch to MD5Hash() or SHA256Hash()

    # Create consistent hash ring with 100 replicas per node
    ring = ConsistentHashRing(hash_strategy=hash_strategy, replicas=100)

    # Add some nodes (e.g., servers)
    ring.add_node("ServerA")
    ring.add_node("ServerB")
    ring.add_node("ServerC")

    print("Active Nodes:", ring.list_nodes(), "\n")

    # Example keys
    keys = ["user:101", "user:102", "user:103", "file:alpha", "file:beta"]

    print("Key → Responsible Node mapping:\n")
    for key in keys:
        print(f"{key} → {ring.get_node(key)}")

    # Remove one node and check remapping
    print("\nRemoving 'ServerB'...\n")
    ring.remove_node("ServerB")

    for key in keys:
        print(f"{key} → {ring.get_node(key)}")


