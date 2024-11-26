import requests, json, sqlite3, time
from threading import Thread
from typing import Sequence
from dataclasses import dataclass, field
from queue import Empty, Full, Queue, ShutDown


@dataclass(frozen=True, kw_only=True)
class SpiderReponse:
    url: str
    status_code: int
    reason: str
    headers: dict = field(default_factory=dict)

    @classmethod
    def from_response(cls, url: str, res: requests.Response):
        headers = {k: v for k, v in res.headers.lower_items()}
        return cls(
            url=url,
            status_code=res.status_code,
            reason=res.reason,
            headers=headers,
        )


def check(url, *, timeout: int = 10):
    try:
        res = requests.head(url, timeout=timeout)
        return SpiderReponse.from_response(url, res)
    except requests.exceptions.Timeout as e:
        return SpiderReponse(url=url, status_code=1000, reason="timeout")
    except Exception as e:
        return SpiderReponse(url=url, status_code=1010, reason=str(e))


def flush(batch: Sequence[SpiderReponse]):
    print(f"flushing {len(batch)}")
    con.executemany(
        """
        INSERT OR REPLACE INTO survey (website, status_code, reason, headers)
        VALUES (?, ?, ?, ?)
        """,
        [
            (
                r.url,
                r.status_code,
                r.reason,
                json.dumps(r.headers),
            )
            for r in batch
        ],
    )
    con.commit()


def take[T](queue: Queue[T]) -> list[T]:
    batch: list[T] = []
    while True:
        try:
            batch.append(queue.get(block=False))
            queue.task_done()
        except Empty:
            return batch


def worker():
    while True:
        try:
            url = work_queue.get(block=True)
        except ShutDown:
            break

        res = check(url)
        print(f"  {res.status_code:4}: {url}")
        result_queue.put(res)
        work_queue.task_done()


BATCH_SIZE = 1000
DB = "/data/duckdb/fs-links.db"

con = sqlite3.connect(DB)
con.autocommit = False
# con.executescript("DROP TABLE IF EXISTS survey;")
con.executescript(
    """
    CREATE TABLE IF NOT EXISTS survey (
        website TEXT PRIMARY KEY,
        status_code INTEGER,
        reason TEXT,
        headers TEXT
    );

    CREATE INDEX IF NOT EXISTS survey_status_code ON survey(status_code);
    """
)

con.commit()

work_queue = Queue[str](maxsize=BATCH_SIZE * 5)
result_queue = Queue[SpiderReponse](maxsize=BATCH_SIZE * 5)

todo = con.execute(
    """
    SELECT ws.website 
      FROM website ws
      LEFT JOIN survey sv ON ws.website = sv.website
     WHERE sv.status_code IS NULL
    """
)

print("starting workers")
for _ in range(BATCH_SIZE):
    Thread(target=worker).start()

print("feeding workers")
for (url,) in todo:
    while True:
        if result_queue.full():
            flush(take(result_queue))

        try:
            work_queue.put(url, block=False)
            break
        except Full:
            time.sleep(0.1)

print("waiting for workers to finish")
work_queue.shutdown()
work_queue.join()

flush(take(result_queue))
