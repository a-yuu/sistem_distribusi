import httpx
import asyncio

async def broadcast_invalidate(key: str):
    """
    Mengirimkan request invalidate cache ke semua peer.
    Kita tidak perlu menunggu respons (fire and forget).
    """
    from src.utils.config import get_settings
    settings = get_settings()
    
    async with httpx.AsyncClient() as client:
        tasks = []
        for peer in settings.peers:
            url = f"{peer}/cache/invalidate/{key}"
            tasks.append(client.post(url, timeout=0.5))
        
        # Jalankan semua request secara paralel
        # Kita tidak peduli hasilnya, yang penting terkirim
        await asyncio.gather(*tasks, return_exceptions=True)