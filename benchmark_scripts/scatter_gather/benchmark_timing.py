from pathlib import Path


def write_timing(task_name: str, start_ns: int, end_ns: int) -> None:
    """
    Schreibt pro Task eine eigene Timing-Datei im aktuellen Task-Arbeitsordner.
    Diese Datei wird erst nach dem gemessenen Task-Intervall erzeugt.
    """
    timing_file = Path(f"timing_{task_name}.txt")

    timing_file.write_text(
        f"task_name={task_name}\n"
        f"task_start_ns={start_ns}\n"
        f"task_end_ns={end_ns}\n"
    )
