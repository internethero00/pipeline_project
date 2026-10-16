import hashlib
import math
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed


# Для каждого воркера это свои имена, ссылающиеся на ОБЩИЕ Value и Lock.
_cpu_counter = None
_cpu_lock = None


def init_cpu_worker(shared_counter, shared_lock) -> None:
    """Один раз передать разделяемые объекты каждому процессу пула."""
    global _cpu_counter, _cpu_lock
    _cpu_counter = shared_counter
    _cpu_lock = shared_lock


def analyze_post(post: dict) -> dict:
    """Вычислить метрики поста и увеличить счётчик успешной обработки."""
    complexity_score = sum(math.sin(i) for i in range(1_000_000)) * len(post["body"])
    text_hash = hashlib.sha256(
        (post["title"] + post["body"]).encode("utf-8")
    ).hexdigest()

    analyzed = {
        **post,
        "complexity_score": complexity_score,
        "text_hash": text_hash,
    }

    with _cpu_lock:
        _cpu_counter.value += 1

    return analyzed


def cpu_monitor(monitor_queue) -> None:
    """Отдельный процесс: выводит ID обработанных постов из очереди."""
    while True:
        post_id = monitor_queue.get()
        if post_id is None:  # Сигнал завершения
            break
        print(f"[Monitor] Пост {post_id} обработан", flush=True)


def process_posts(posts: list[dict]) -> list[dict]:
    """Выполнить CPU-обработку параллельно и уведомлять монитор."""
    if not posts:
        return []

    counter = multiprocessing.Value("i", 0)
    lock = multiprocessing.Lock()
    monitor_queue = multiprocessing.Queue()

    monitor_process = multiprocessing.Process(
        target=cpu_monitor,
        args=(monitor_queue,),
    )
    monitor_process.start()

    results = []
    try:
        with ProcessPoolExecutor(
            initializer=init_cpu_worker,
            initargs=(counter, lock),
        ) as executor:
            futures = {
                executor.submit(analyze_post, post): post["id"]
                for post in posts
            }

            for future in as_completed(futures):
                post_id = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    print(f"[CPU] Ошибка анализа поста {post_id}: {exc}")
                    continue

                results.append(result)
                # ID помещает в очередь MAIN, не worker.
                monitor_queue.put(result["id"])
    finally:
        monitor_queue.put(None)
        monitor_process.join()
        monitor_queue.close()
        monitor_queue.join_thread()

    results.sort(key=lambda post: post["id"])
    print(f"[CPU] Успешно обработано: {counter.value}")
    return results