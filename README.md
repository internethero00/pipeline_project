
# Pipeline Project

Учебный Python-проект с использованием asyncio, multiprocessing и threading.

## Структура проекта

```text
pipeline_project/
├── main.py
├── fetcher.py
├── analyzer.py
├── saver.py
├── results.log
└── README.md
```

## Запуск проекта

Создать виртуальное окружение:

```bash
python -m venv .venv
```

Активировать окружение:

```bash
source .venv/bin/activate
```

Установить зависимости:

```bash
pip install httpx
```

Запустить:

```bash
python main.py
```

## Используемые технологии

### 1. asyncio + httpx

Асинхронная загрузка 100 постов.

- `AsyncClient` — HTTP-клиент с пулом соединений.
- `Semaphore(5)` — максимум 5 одновременных запросов.
- `gather()` — конкурентное выполнение корутин.

### 2. multiprocessing

Параллельная CPU-bound обработка постов.

- `ProcessPoolExecutor` — пул процессов.
- `Value` — общий счётчик.
- `Lock` — защита от race condition.
- `Queue` — передача ID обработанных постов.
- `Process` — отдельный процесс мониторинга.

### 3. threading

Сохранение результатов в файл `results.log`.

- `ThreadPoolExecutor` — пул из 3 потоков.
- `queue.Queue` — очередь задач.
- `threading.Lock` — безопасная запись в файл.
- `threading.Event` — сигнал завершения.
- `threading.Thread` — мониторинг прогресса.

## Итоговый отчёт

```text
=== Отчёт о работе пайплайна ===
Запрошено постов: 100
Успешно загружено: 98
Успешно обработано (CPU): 98
Успешно сохранено (I/O): 98
Общее время: 4.32 сек.
```

Числа приведены для примера.
