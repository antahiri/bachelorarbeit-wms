from pathlib import Path


def write_timing(task_name: str, start_ns: int, end_ns: int) -> None:
    Path(f"timing_{task_name}.txt").write_text(
        f"task_name={task_name}\n"
        f"task_start_ns={start_ns}\n"
        f"task_end_ns={end_ns}\n"
    )
