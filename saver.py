
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


RESULTS_FILE = Path(__file__).resolve().parent / "results.log"
THREAD_COUNT = 3


def save_post(post_data: dict, file_lock: threading.Lock) -> None:
    """Имитация медленного диска + атомарная по lock запись строки."""
    time.sleep(0.05)

    line = (
        f"ID: {post_data['id']} | "
        f"Title: {post_data['title']} | "
        f"Score: {post_data['complexity_score']:.4f} | "
        f"SHA256: {post_data['text_hash']}\n"
    )

    with file_lock:
        with RESULTS_FILE.open("a", encoding="utf-8") as file:
            file.write(line)


def save_worker(
    task_queue: queue.Queue,
    file_lock: threading.Lock,
    stop_event: threading.Event,
    saved_counter: list[int],
) -> None:
    """Долго живущий поток: получает посты из общей очереди."""
    while not stop_event.is_set():
        try:
            post = task_queue.get(timeout=0.2)
        except queue.Empty:
            continue

        try:
            save_post(post, file_lock)
            with file_lock:
                saved_counter[0] += 1
        except Exception as exc:
            print(f"[I/O] Ошибка записи поста {post.get('id')}: {exc}")
        finally:
            task_queue.task_done()


def print_save_progress(
    stop_event: threading.Event,
    saved_counter: list[int],
    file_lock: threading.Lock,
    total: int,
) -> None:
    """Печатает прогресс каждые две секунды, пока Event не установлен."""
    while not stop_event.wait(2.0):
        with file_lock:
            saved = saved_counter[0]
        print(f">>> Прогресс сохранения: {saved} / {total}")


def save_posts(posts: list[dict]) -> int:
    """Сохранить обработанные посты тремя потоками через queue.Queue."""
    task_queue = queue.Queue()
    file_lock = threading.Lock()
    stop_event = threading.Event()
    saved_counter = [0]

    for post in posts:
        task_queue.put(post)

    progress_thread = threading.Thread(
        target=print_save_progress,
        args=(stop_event, saved_counter, file_lock, len(posts)),
        name="save-progress",
    )
    progress_thread.start()

    executor = ThreadPoolExecutor(max_workers=THREAD_COUNT)
    worker_futures = []
    try:
        worker_futures = [
            executor.submit(
                save_worker,
                task_queue,
                file_lock,
                stop_event,
                saved_counter,
            )
            for _ in range(THREAD_COUNT)
        ]
        task_queue.join()  # Ждём task_done() для каждого добавленного поста.
    finally:
        stop_event.set()  # Разрешаем воркерам и монитору завершиться.
        executor.shutdown(wait=True)
        progress_thread.join()

    for future in worker_futures:
        future.result()  # Покажет неожиданные ошибки воркеров, если они были.

    print(f">>> Прогресс сохранения: {saved_counter[0]} / {len(posts)}")
    return saved_counter[0]
