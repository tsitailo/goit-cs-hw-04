import time
from collections import defaultdict
from multiprocessing import Process, Queue
from pathlib import Path
from threading import Thread, Lock


def get_file_chunks(file_paths: list[str], num_threads: int) -> list[list[str]]:
    """
    Розбиває список файлів на частини для розподілу між потоками.
    """
    num_threads = min(num_threads, len(file_paths))
    chunk_size = (len(file_paths) + num_threads - 1) // num_threads
    file_chunks = [file_paths[i:i + chunk_size] for i in range(0, len(file_paths), chunk_size)]

    return file_chunks


def search_keywords_in_file(file_path: str, keywords: list[str]) -> dict[str, list[str]]:
    """
    Шукає задані ключові слова в одному файлі.
    Повертає словник, де ключ — ключове слово, а значення — список із шляхом до файлу.
    """

    found_keywords = defaultdict(list)
    try:

        path = Path(file_path)
        if not path.is_file():
            print(f"Помилка: Файл не існує або це не файл: {file_path}")
            return found_keywords

        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read().lower()
            for keyword in keywords:
                if keyword.lower() in content:
                    found_keywords[keyword].append(str(file_path))

    except (OSError, UnicodeDecodeError, PermissionError) as e:
        print(f"Помилка під час читання файлу {file_path}: {e}")
    except Exception as e:
        print(f"Несподівана помилка при обробці файлу {file_path}: {e}")

    return found_keywords


# ==========================================
# 1. Багатопотоковий підхід (threading)
# ==========================================

def _worker_thread(file_paths: list[str], keywords: list[str], results: dict[str, list[str]], lock: Lock):
    """
    Функція-воркер для потоку. Обробляє частину файлів та безпечно оновлює спільний словник.
    """
    local_results = defaultdict(list)
    for file_path in file_paths:
        file_results = search_keywords_in_file(file_path, keywords)
        for kw, paths in file_results.items():
            local_results[kw].extend(paths)

    with lock:
        for kw, paths in local_results.items():
            results[kw].extend(paths)


def search_keywords_threading(file_paths: list[str], keywords: list[str], num_threads: int = 4) -> dict[str, list[str]]:
    """
    Багатопотоковий пошук ключових слів у файлах.
    """
    if not file_paths:
        return {kw: [] for kw in keywords}

    results = defaultdict(list)
    lock = Lock()
    threads = []
    file_chunks = get_file_chunks(file_paths, num_threads)

    start_time = time.time()

    for chunk in file_chunks:
        thread = Thread(target=_worker_thread, args=(chunk, keywords, results, lock))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    execution_time = time.time() - start_time
    print(f"Час виконання: {execution_time:.6f} сек")

    return {kw: results.get(kw, []) for kw in keywords}


# ==========================================
# 2. Багатопроцесорний підхід (multiprocessing)
# ==========================================

def _worker_process(file_paths: list[str], keywords: list[str], queue: Queue):
    """
    Функція-воркер для процесу. Обробляє частину файлів та передає результати через Queue.
    """
    local_results = defaultdict(list)
    for file_path in file_paths:
        file_results = search_keywords_in_file(file_path, keywords)
        for kw, paths in file_results.items():
            local_results[kw].extend(paths)

    queue.put(dict(local_results))


def search_keywords_multiprocessing(file_paths: list[str], keywords: list[str], num_processes: int = 4) -> dict[
    str, list[str]]:
    """
    Багатопроцесорний пошук ключових слів у файлах за допомогою Queue.
    """
    if not file_paths:
        return {kw: [] for kw in keywords}

    queue = Queue()
    processes = []
    file_chunks = get_file_chunks(file_paths, num_processes)

    start_time = time.time()

    for chunk in file_chunks:
        process = Process(target=_worker_process, args=(chunk, keywords, queue))
        processes.append(process)
        process.start()

    merged_results = defaultdict(list)
    for _ in range(len(file_chunks)):
        process_result = queue.get()
        for kw, paths in process_result.items():
            merged_results[kw].extend(paths)

    for process in processes:
        process.join()

    execution_time = time.time() - start_time
    print(f"Час виконання: {execution_time:.6f} сек")

    return {kw: merged_results.get(kw, []) for kw in keywords}


if __name__ == "__main__":
    # Створення тестових файлів для перевірки
    test_dir = Path("test_files")
    test_dir.mkdir(exist_ok=True)

    test_data = {
        "file1.txt": "Python is an interpreted high-level general-purpose programming language.",
        "file2.txt": "Multiprocessing supports spawning processes using an API similar to threading.",
        "file3.txt": "Threading is best suited for I/O-bound tasks in Python due to the GIL.",
        "file4.txt": "Keywords search algorithm should be fast and handle errors gracefully.",
        "file5.txt": "Створіть дві версії програми: одну — з використанням модуля threading для багатопотокового програмування, та іншу — з використанням модуля multiprocessing для багатопроцесорного програмування.",
        "file6.txt": "Розробіть програму, яка паралельно обробляє та аналізує текстові файли для пошуку визначених ключових слів."
    }

    test_file_paths = []
    for filename, text in test_data.items():
        file_path = test_dir / filename
        file_path.write_text(text, encoding="utf-8")
        test_file_paths.append(str(file_path))

    keywords_to_search = ["python", "threading", "multiprocessing", "errors", "missing_word"]

    print("MULTITHREADING")
    threading_res = search_keywords_threading(test_file_paths, keywords_to_search)
    print("Результати:", threading_res, "\n")

    print("MULTIPROCESSING")
    multiprocessing_res = search_keywords_multiprocessing(test_file_paths, keywords_to_search)
    print("Результати:", multiprocessing_res)
