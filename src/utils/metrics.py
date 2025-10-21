# Berkas ini akan menyimpan metrics performa, seperti cache hits/misses.
# Kita gunakan class sederhana agar mudah di-import dan diubah.

class CacheMetrics:
    def __init__(self):
        self.cache_hits = 0
        self.cache_misses = 0

    def hit(self):
        self.cache_hits += 1

    def miss(self):
        self.cache_misses += 1

    def get_stats(self):
        return {"cache_hits": self.cache_hits, "cache_misses": self.cache_misses}

# Buat satu instance global agar bisa diakses dari mana saja
metrics_store = CacheMetrics()