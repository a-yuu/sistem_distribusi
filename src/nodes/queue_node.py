import redis
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from src.utils.config import get_settings
from src.utils.hashing import ConsistentHasher

# Dapatkan settings
settings = get_settings()

# Inisialisasi Redis Client
try:
    redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=0,
        decode_responses=True # Penting agar output Redis berupa string
    )
    redis_client.ping()
    print(f"✅ Node {settings.node_id} berhasil terhubung ke Redis.")
except redis.exceptions.ConnectionError as e:
    print(f"❌ Node {settings.node_id} GAGAL terhubung ke Redis: {e}")
    redis_client = None

# Inisialisasi Consistent Hashing Ring
# Kita gunakan node_id sebagai nama node di ring
node_ids = [url.split('//')[1].split(':')[0] for url in settings.all_nodes]
hasher = ConsistentHasher(nodes=node_ids)

# Fungsi untuk mem-forward request ke node yang benar
async def forward_request(node_id: str, request: Request):
    """Meneruskan request ke node yang bertanggung jawab."""
    # Dapatkan URL lengkap dari node_id
    node_url = next((url for url in settings.all_nodes if node_id in url), None)
    if not node_url:
        raise HTTPException(status_code=500, detail="Node not found in config")

    async with httpx.AsyncClient() as client:
        try:
            # Bangun ulang URL untuk di-forward
            url_path = request.url.path
            fwd_url = f"{node_url}{url_path}"
            
            # Kirim request persis seperti aslinya
            fwd_response = await client.request(
                method=request.method,
                url=fwd_url,
                headers=request.headers,
                content=await request.body()
            )
            return fwd_response.json(), fwd_response.status_code
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Gagal meneruskan request ke {node_id}: {e}")

def add_queue_routes(app: FastAPI):
    
    @app.post("/queue/{queue_name}")
    async def produce(queue_name: str, message: dict, request: Request):
        """
        Menambahkan pesan ke antrean.
        Request akan di-forward jika node ini tidak bertanggung jawab.
        """
        if not redis_client:
            raise HTTPException(status_code=503, detail="Service Redis tidak tersedia")
        
        # 1. Tentukan node yang bertanggung jawab atas antrean ini
        responsible_node = hasher.get_node(queue_name)
        
        # 2. Jika bukan node ini, forward request-nya
        if responsible_node != settings.node_id:
            print(f"Node {settings.node_id} meneruskan request queue {queue_name} ke {responsible_node}")
            json_response, status_code = await forward_request(responsible_node, request)
            return JSONResponse(content=json_response, status_code=status_code)

        # 3. Jika ini node yang benar, proses
        try:
            print(f"Node {settings.node_id} memproses queue {queue_name}")
            redis_client.lpush(queue_name, str(message))
            return {"status": "message produced", "queue": queue_name, "node": settings.node_id, "message": message}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/queue/{queue_name}")
    async def consume(queue_name: str, request: Request):
        """
        Mengambil pesan dari antrean (at-least-once delivery).
        Request akan di-forward jika node ini tidak bertanggung jawab.
        """
        if not redis_client:
            raise HTTPException(status_code=503, detail="Service Redis tidak tersedia")

        responsible_node = hasher.get_node(queue_name)
        
        if responsible_node != settings.node_id:
            print(f"Node {settings.node_id} meneruskan request queue {queue_name} ke {responsible_node}")
            json_response, status_code = await forward_request(responsible_node, request)
            return JSONResponse(content=json_response, status_code=status_code)

        try:
            print(f"Node {settings.node_id} memproses queue {queue_name}")
            
            # Pola At-Least-Once Delivery [cite: 1071]
            # Pindahkan pesan ke antrean 'processing' sementara
            processing_queue = f"{queue_name}:processing"
            message = redis_client.rpoplpush(queue_name, processing_queue)
            
            if message:
                # Berikan pesan dan token (nama antrean processing) ke klien
                # Klien harus call /queue/ack/{queue_name} untuk konfirmasi
                return {
                    "status": "message consumed", 
                    "node": settings.node_id,
                    "message": eval(message), # eval() mengubah string dict kembali jadi dict
                    "ack_token": processing_queue # Klien butuh ini untuk ACK
                }
            else:
                raise HTTPException(status_code=404, detail="Queue is empty or does not exist")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/queue/ack/{processing_queue}")
    async def acknowledge(processing_queue: str, message: dict):
        """
        Konfirmasi bahwa pesan telah selesai diproses (menghapusnya dari antrean processing).
        """
        if not redis_client:
            raise HTTPException(status_code=503, detail="Service Redis tidak tersedia")
        
        try:
            # Hapus pesan spesifik dari antrean processing
            # LREM 0 = hapus semua instance dari value
            redis_client.lrem(processing_queue, 0, str(message))
            return {"status": "message acknowledged"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))