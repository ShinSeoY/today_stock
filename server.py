import threading
import asyncio
from src.consumer import start_consumer

if __name__ == "__main__":
    asyncio.run(start_consumer())