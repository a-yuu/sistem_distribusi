import hashlib
import bisect

class ConsistentHasher:
    """
    Implementasi Consistent Hashing sederhana.
    Ini akan memetakan kunci (seperti nama antrean) ke salah satu node.
    """
    def __init__(self, nodes: list[str] = None, replicas=5):
        self.replicas = replicas
        self._ring = dict()
        self._sorted_keys = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node: str):
        """Menambahkan node ke hash ring"""
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            self._ring[key] = node
            self._sorted_keys.append(key)
        self._sorted_keys.sort()

    def get_node(self, item_key: str) -> str | None:
        """
        Mendapatkan node yang paling 'bertanggung jawab' untuk item_key.
        """
        if not self._ring:
            return None
        
        key = self._hash(item_key)
        
        # Cari posisi node terdekat di dalam ring
        idx = bisect.bisect(self._sorted_keys, key)
        
        # wrap-around jika key lebih besar dari semua node
        idx = idx % len(self._sorted_keys)
        
        return self._ring[self._sorted_keys[idx]]

    def _hash(self, key: str) -> int:
        """Hash string menjadi integer"""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)