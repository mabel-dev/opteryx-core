# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Asynchronous trace file writer.

Writes trace events to a JSONLines file (.jsonl) in the background without
blocking query execution.
"""

import json
import queue
import threading
import time
from pathlib import Path
from typing import Optional


class TraceWriter:
    """
    Optional helper class providing asynchronous file output for trace
    events.  The engine no longer uses this by default; it is retained for
    backwards compatibility and for users who wish to install a custom sink.
    """

    def __init__(self, output_file: str, max_queue_size: int = 100000):
        """
        Initialize the trace writer.

        Args:
            output_file: Path to write .jsonl trace file
            max_queue_size: Maximum events to buffer before blocking enqueue
        """
        self.output_file = Path(output_file)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        # Log that tracing is enabled
        print(f"[TRACE] Initializing IO trace writer to: {self.output_file}")

        self.queue = queue.Queue(maxsize=max_queue_size)
        self.running = True
        self.file_handle: Optional[object] = None
        self.events_written = 0
        self._summary_printed = False

        # Start background writer thread (non-daemon so it flushes on exit)
        self.writer_thread = threading.Thread(target=self._writer_loop, daemon=False)
        self.writer_thread.start()

    def enqueue_events(self, events: list) -> None:
        """
        Add events to the write queue.

        If queue is full, silently drops the event to avoid blocking the query.
        A warning could be logged here in a real implementation.

        Args:
            events: List of event dictionaries to write
        """
        for event in events:
            try:
                # Non-blocking put - if queue is full, drop the event
                self.queue.put_nowait(event)
            except queue.Full:
                # Queue is full, drop this event to avoid blocking query
                # In production, could log a warning here
                pass

    def _writer_loop(self) -> None:
        """Background thread that writes events to disk."""
        try:
            with open(self.output_file, "a", encoding="utf-8") as f:
                self.file_handle = f

                while self.running:
                    try:
                        event = self.queue.get(timeout=0.1)
                        if event.get("type") != "_flush_sentinel":
                            f.write(json.dumps(event) + "\n")
                            self.events_written += 1
                        if self.queue.qsize() == 0:
                            f.flush()
                    except queue.Empty:
                        f.flush()

                # Drain any remaining events after running=False
                while True:
                    try:
                        event = self.queue.get_nowait()
                        if event.get("type") != "_flush_sentinel":
                            f.write(json.dumps(event) + "\n")
                            self.events_written += 1
                    except queue.Empty:
                        break
                f.flush()

        except Exception as e:
            import traceback

            print(f"Error in trace writer: {e}")
            traceback.print_exc()

    def flush(self) -> None:
        """
        Flush all pending events and wait for completion.

        Blocks until all events in queue have been written to disk.
        """
        # Signal writer to process all remaining events
        sentinel = {"type": "_flush_sentinel"}
        self.queue.put(sentinel)

        # Wait for sentinel to be processed
        timeout = 10  # seconds
        start = time.time()
        while self.queue.qsize() > 0:
            if time.time() - start > timeout:
                print(f"Warning: trace writer flush timeout after {timeout}s")
                break
            time.sleep(0.01)

        # Give writer thread time to process
        time.sleep(0.1)

        # Print final summary with instructions (only once)
        if not self._summary_printed:
            self._summary_printed = True
            if self.events_written > 0:
                print(f"[TRACE] Wrote {self.events_written} trace events to {self.output_file}")
                # show sampling information if relevant
                try:
                    from opteryx import config as _cfg

                    if _cfg.OPTERYX_TRACE_SAMPLE_RATE != 1.0:
                        print(f"[TRACE] Sampling rate: {_cfg.OPTERYX_TRACE_SAMPLE_RATE}")
                except Exception:
                    pass
                print(f"[TRACE] View waterfall chart:")
                print(f"[TRACE]   python -m opteryx.tools.io_waterfall trace {self.output_file}")
                print(f"[TRACE] View statistics:")
                print(f"[TRACE]   python -m opteryx.tools.io_waterfall stats {self.output_file}")
            else:
                print(f"[TRACE] Trace file written: {self.output_file} (no IO events recorded)")

    def close(self) -> None:
        """Close the trace writer and stop the background thread."""
        self.running = False
        self.writer_thread.join(timeout=5.0)
        if self.file_handle:
            self.file_handle.close()
