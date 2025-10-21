from fastapi import FastAPI, BackgroundTasks, HTTPException
from functools import lru_cache

from src.utils.metrics import metrics_store
from src.communication.message_passing import broadcast_invalidate

# Ini adalah 'database' palsu kita.
# Di dunia nyata, ini adalah database SQL atau NoSQL.
mock_db = {
    "item:123": "Ini adalah data untuk item 123",
    "item:456": "Data rahasia untuk item 456"
}

# --- Implementasi Cache (LRU) ---
# Kita gunakan @lru_cache untuk memenuhi syarat LRU Policy [cite: 36]
@lru_cache(maxsize=128)
def get_data_from_db(key: str) -> str | None:
    """
    Fungsi ini mengambil data dari 'database' palsu.
    @lru_cache akan otomatis menyimpan hasilnya.
    """
    # Catat sebagai cache miss
    metrics_store.miss()
    print(f"CACHE MISS: Mengambil data '{key}' dari DB.")
    return mock_db.get(key)

# --- Fungsi untuk Rute API ---
def add_cache_routes(app: FastAPI):

    @app.get("/cache/{key}")
    def read_cache(key: str):
        """
        Membaca data. Akan mencoba dari cache terlebih dahulu.
        """
        data = get_data_from_db(key)
        
        # Cek apakah @lru_cache berhasil menemukan data
        # Jika cache_info().hits > 0, berarti data ada di cache
        if get_data_from_db.cache_info().hits > 0:
            metrics_store.hit() # Catat sebagai cache hit
        
        if data:
            return {"key": key, "data": data, "source": "cache (via LRU)"}
        else:
            raise HTTPException(status_code=404, detail="Data not found")

    @app.post("/cache/{key}")
    async def write_cache(key: str, value: dict, background_tasks: BackgroundTasks):
        """
        Menulis/memperbarui data.
        Ini akan meng-invalidate cache di semua node lain.
        """
        # 1. Tulis data ke 'database' palsu
        mock_db[key] = value.get("data")
        
        # 2. Hapus cache LOKAL untuk key ini
        get_data_from_db.cache_clear() # Cara simpel, atau bisa invalidasi per key
        
        # 3. Kirim invalidasi ke semua PEER 
        # Kita gunakan BackgroundTasks agar request ini tidak memblokir respons
        background_tasks.add_task(broadcast_invalidate, key)
        
        return {"status": "data updated", "key": key, "new_data": value.get("data")}

    @app.post("/cache/invalidate/{key}")
    def invalidate_cache(key: str):
        """
        Endpoint internal yang dipanggil oleh node lain
        untuk meng-invalidate cache mereka.
        """
        print(f"INVALIDATE diterima untuk key: {key}")
        # Hapus cache LOKAL untuk key ini
        get_data_from_db.cache_clear() # Cara simpel
        return {"status": "cache invalidated", "key": key}

    @app.get("/metrics")
    def get_metrics():
        """
        Endpoint untuk memenuhi syarat Performance Monitoring 
        """
        stats = metrics_store.get_stats()
        stats["cache_info"] = str(get_data_from_db.cache_info())
        return stats