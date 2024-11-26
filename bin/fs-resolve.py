import requests, json, sqlite3
import time
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass, field
from queue import Empty, Queue


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


@dataclass(frozen=True, kw_only=True)
class SpiderWorkerConfig:
    exe: Executor
    max_active: int = 100


class SpiderWorker(SpiderWorkerConfig):
    active: int = 0

    def _done(self, fut):
        self.active -= 1

    def schedule(self, url):
        while self.active >= self.max_active:
            time.sleep(0.1)

        self.active += 1
        fut = self.exe.submit(check, url)
        fut.add_done_callback(self._done)
        return fut


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
    """
)

con.commit()

BATCH_SIZE = 1000

exe = ThreadPoolExecutor(max_workers=BATCH_SIZE)
worker = SpiderWorker(exe=exe, max_active=BATCH_SIZE * 5)

queue = Queue[SpiderReponse](maxsize=BATCH_SIZE)


def flush():
    batch = []
    while True:
        try:
            next = queue.get(block=False)
        except Empty:
            break
        batch.append(next)
        queue.task_done()

    if not batch:
        return

    print(f"flushing {len(batch)} records")

    con.executemany(
        """
        INSERT INTO survey (website, status_code, reason, headers)
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


def take_result(fut):
    res = fut.result()
    queue.put(res)
    print(f"  {res.url} -> {res.status_code}")


todo = con.execute(
    """
    SELECT ws.website 
      FROM website ws
      LEFT JOIN survey sv ON ws.website = sv.website
     WHERE sv.status_code IS NULL
    """
)

for (url,) in todo:
    if queue.full():
        flush()

    while worker.active >= worker.max_active:
        time.sleep(0.1)
        if queue.full():
            flush()

    fut = worker.schedule(url)
    fut.add_done_callback(take_result)


print("Waiting for all workers to finish")
while worker.active > 0:
    time.sleep(0.1)

flush()
con.close()
