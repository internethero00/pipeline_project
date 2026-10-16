import asyncio
import time

from analyzer import process_posts
from fetcher import POST_COUNT, fetch_all_posts
from saver import save_posts


def run_pipeline() -> dict:
    started_at = time.perf_counter()

    downloaded = asyncio.run(fetch_all_posts())
    posts = [post for post in downloaded if post is not None]
    analyzed_posts = process_posts(posts)
    saved_count = save_posts(analyzed_posts)

    elapsed = time.perf_counter() - started_at
    report = {
        "requested": POST_COUNT,
        "downloaded": len(posts),
        "analyzed": len(analyzed_posts),
        "saved": saved_count,
        "elapsed_seconds": elapsed,
    }

    print("\n=== Отчёт о работе пайплайна ===")
    print(f"Запрошено постов: {report['requested']}")
    print(f"Успешно загружено: {report['downloaded']}")
    print(f"Успешно обработано (CPU): {report['analyzed']}")
    print(f"Успешно сохранено (I/O): {report['saved']}")
    print(f"Общее время: {report['elapsed_seconds']:.2f} сек.")
    return report


if __name__ == "__main__":
    run_pipeline()
