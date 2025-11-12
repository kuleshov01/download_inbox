# -*- coding: utf-8 -*-
"""
Утилита для дублирования консольного вывода в лог-файл.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, TextIO, TypeVar

T = TypeVar("T")


class _TeeStream:
    """
    Простейший "тройник" для одновременной записи в несколько потоков.
    """

    def __init__(self, *streams: TextIO):
        self._streams = tuple(streams)

    def write(self, data: str) -> int:
        for stream in self._streams:
            stream.write(data)
        return len(data)

    def flush(self) -> None:
        for stream in self._streams:
            stream.flush()

    def isatty(self) -> bool:
        return any(getattr(stream, "isatty", lambda: False)() for stream in self._streams)


def _sanitize_component(value: Optional[str], default: str) -> str:
    if not value:
        return default
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value.strip())
    return cleaned or default


def _build_log_path(output_dir: Optional[str], script_name: Optional[str],
                    date_start: Optional[str], date_end: Optional[str]) -> Path:
    base_dir = Path(output_dir or os.getcwd())
    log_dir = base_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    name_component = _sanitize_component(script_name, "script")
    range_component = f"{_sanitize_component(date_start, 'unknown')}_{_sanitize_component(date_end, 'unknown')}"
    timestamp_component = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name_component}_{range_component}_{timestamp_component}.log"
    return log_dir / filename


def capture_console_output(
    *,
    output_dir: Optional[str],
    date_start: Optional[str],
    date_end: Optional[str],
    script_name: Optional[str],
    func: Callable[..., T],
    args: Optional[tuple[Any, ...]] = None,
    kwargs: Optional[dict[str, Any]] = None,
) -> T:
    """
    Запускает функцию и записывает stdout/stderr в лог-файл, сохраняя вывод на консоли.
    """
    args = args or ()
    kwargs = kwargs or {}

    log_path = _build_log_path(output_dir, script_name, date_start, date_end)

    original_stdout, original_stderr = sys.stdout, sys.stderr

    with open(log_path, "w", encoding="utf-8") as log_file:
        tee_stdout = _TeeStream(original_stdout, log_file)
        tee_stderr = _TeeStream(original_stderr, log_file)

        sys.stdout, sys.stderr = tee_stdout, tee_stderr
        try:
            print(f"[console_logger] Логи сохраняются в: {log_path}")
            result = func(*args, **kwargs)
            print(f"[console_logger] Выполнение '{script_name or 'script'}' завершено.")
            return result
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            sys.stdout, sys.stderr = original_stdout, original_stderr
