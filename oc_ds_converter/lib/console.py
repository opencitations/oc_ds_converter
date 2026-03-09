# -*- coding: utf-8 -*-
# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

from datetime import timedelta
from math import ceil

from rich.console import Console
from rich.progress import BarColumn, Progress, ProgressColumn, SpinnerColumn, Task, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.text import Text

console = Console()


class EMATimeRemainingColumn(ProgressColumn):
    """Time remaining column using Exponential Moving Average for stable estimates.

    Rich's default TimeRemainingColumn uses a simple windowed average that becomes
    unstable with infrequent updates. This implementation uses EMA (like tqdm) to
    provide smoother estimates by weighting recent observations more while retaining
    historical information.

    EMA formula: EMA_new = α × current_value + (1 - α) × EMA_previous

    With α = 0.3 (default):
    - 30% weight to the newly measured speed
    - 70% weight to the historical average (which itself contains 70% of previous
      history, creating exponential decay of older values)
    """

    # Limit refresh rate to avoid excessive recalculations
    max_refresh = 0.5

    def __init__(self, smoothing: float = 0.3):
        # α (alpha): smoothing factor between 0 and 1
        # - Lower values (e.g., 0.1) = more stable but slower to react
        # - Higher values (e.g., 0.5) = more reactive but potentially volatile
        self.smoothing = smoothing
        # Store EMA speed estimate per task (supports multiple progress bars)
        self._ema_speed: dict[int, float] = {}
        # Store previous state to calculate instantaneous speed
        self._last_completed: dict[int, float] = {}
        self._last_time: dict[int, float] = {}
        super().__init__()

    def render(self, task: Task) -> Text:
        if task.finished:
            return Text("0:00:00", style="progress.remaining")
        if task.total is None or task.remaining is None:
            return Text("-:--:--", style="progress.remaining")

        current_time = task.get_time()
        task_id = task.id

        # Calculate instantaneous speed if we have a previous measurement
        if task_id in self._last_time:
            # Time elapsed since last update
            dt = current_time - self._last_time[task_id]
            # Work completed since last update
            dc = task.completed - self._last_completed[task_id]

            if dt > 0 and dc > 0:
                # Instantaneous speed = work done / time taken
                instant_speed = dc / dt

                if task_id in self._ema_speed:
                    # EMA formula: blend new measurement with historical average
                    # EMA = α × instant_speed + (1 - α) × previous_EMA
                    self._ema_speed[task_id] = (
                        self.smoothing * instant_speed
                        + (1 - self.smoothing) * self._ema_speed[task_id]
                    )
                else:
                    # First measurement: use it directly as initial EMA
                    self._ema_speed[task_id] = instant_speed

        # Save current state for next iteration's delta calculation
        self._last_time[task_id] = current_time
        self._last_completed[task_id] = task.completed

        # Calculate time remaining using EMA speed
        speed = self._ema_speed.get(task_id)
        if not speed:
            return Text("-:--:--", style="progress.remaining")

        # time_remaining = work_remaining / speed
        estimate = ceil(task.remaining / speed)
        delta = timedelta(seconds=estimate)
        return Text(str(delta), style="progress.remaining")


def create_progress() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[cyan]{task.completed}/{task.total}[/cyan]"),
        TimeElapsedColumn(),
        EMATimeRemainingColumn(),
        console=console,
    )
