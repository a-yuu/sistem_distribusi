import uvicorn
from fastapi import FastAPI
from dotenv import load_dotenv

# Muat environment variables dari file .env
load_dotenv() 

from src.utils.config import get_settings
# Import fungsi rute dari node lain
from src.nodes.queue_node import add_queue_routes
from src.nodes.cache_node import add_cache_routes

settings = get_settings()
app = FastAPI(title=f"Node: {settings.node_id}")


# --- Hubungkan Rute dari Modul Lain ---
add_queue_routes(app)
add_cache_routes(app)
# add_lock_routes(app)
# --------------------------------------


@app.get("/")
def read_root():
    return {
        "message": f"Hello from Node {settings.node_id}",
        "port": settings.port,
        "peers": settings.peers
    }

if __name__ == "__main__":
    print(f"🚀 Starting Node '{settings.node_id}' on port {settings.port}")
    uvicorn.run(app, host="0.0.0.0", port=settings.port)