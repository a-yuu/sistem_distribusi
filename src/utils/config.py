# src/utils/config.py
import os
from functools import lru_cache

class Settings:
    def __init__(self):
        self.port: int = int(os.getenv("PORT", "8000"))
        self.node_id: str = os.getenv("NODE_ID", "default_node")
        
        nodes_str = os.getenv("ALL_NODES", f"http://localhost:{self.port}")
        self.all_nodes: list[str] = [node.strip() for node in nodes_str.split(',')]
        
        self.self_url = f"http://{self.node_id}:{self.port}"
        
        # Daftar node lain (peers)
        self.peers: list[str] = [node for node in self.all_nodes if node != self.self_url]
        
        self.redis_host: str = os.getenv("REDIS_HOST", "redis")
        self.redis_port: int = int(os.getenv("REDIS_PORT", "6379"))

@lru_cache()
def get_settings():
    return Settings()