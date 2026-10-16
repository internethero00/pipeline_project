
import asyncio

import httpx


BASE_URL = "https://jsonplaceholder.typicode.com/posts"
POST_COUNT = 100
MAX_CONCURRENT_REQUESTS = 5


async def log_start() -> None:
    """Отдельная задача для сообщения о старте загрузки."""
    print(f"[HTTP] Начинаем загрузку {POST_COUNT} постов")


async def fetch_post(
    client: httpx.AsyncClient,
    post_id: int,
    semaphore: asyncio.Semaphore,
) -> dict | None:
    """Загрузить один пост или вернуть None, если запрос не удался."""
    async with semaphore:
        try:
            response = await client.get(f"{BASE_URL}/{post_id}")
            response.raise_for_status()
            post = response.json()
            print(f"[HTTP] Пост {post_id} загружен")
            return post
        except (httpx.HTTPError, ValueError) as exc:
            print(f"[HTTP] Ошибка загрузки поста {post_id}: {exc}")
            return None


async def fetch_all_posts() -> list[dict | None]:
    """Конкурентно загрузить посты с ID 1..100."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with httpx.AsyncClient(timeout=10.0) as client:
        log_task = asyncio.create_task(log_start())
        posts = await asyncio.gather(
            *(
                fetch_post(client, post_id, semaphore)
                for post_id in range(1, POST_COUNT + 1)
            )
        )
        await log_task

    return list(posts)
