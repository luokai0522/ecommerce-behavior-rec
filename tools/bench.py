"""接口压测：统计延迟分位与吞吐（论文 6.4 节性能测试用）。

只依赖标准库，直接对运行中的服务打请求。

用法：
    python tools/bench.py --path "/api/recommend/41?top=10" --requests 500 --concurrency 1
    python tools/bench.py --path "/api/stats" --requests 300 --concurrency 20
"""
from __future__ import annotations

import argparse
import statistics
import sys
import threading
import time
import urllib.request


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="REST 接口压测")
    p.add_argument("--base-url", default="http://127.0.0.1:8000")
    p.add_argument("--path", default="/api/recommend/41?top=10")
    p.add_argument("--requests", type=int, default=500, help="总请求数")
    p.add_argument("--concurrency", type=int, default=1, help="并发线程数")
    p.add_argument("--warmup", type=int, default=20, help="预热请求数（不计入统计）")
    return p.parse_args(argv)


def one(base: str, path: str) -> tuple[float, int]:
    start = time.perf_counter()
    with urllib.request.urlopen(base + path, timeout=10) as resp:
        resp.read()
        code = resp.status
    return (time.perf_counter() - start) * 1000.0, code


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    idx = min(len(values) - 1, int(round(q / 100.0 * (len(values) - 1))))
    return sorted(values)[idx]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        one(args.base_url, "/health")
    except Exception as exc:  # noqa: BLE001
        print(f"[bench] 服务不可用: {exc}", file=sys.stderr)
        return 1

    for _ in range(args.warmup):
        one(args.base_url, args.path)

    latencies: list[float] = []
    errors = 0
    lock = threading.Lock()
    per_thread = args.requests // max(args.concurrency, 1)

    def worker() -> None:
        nonlocal errors
        local: list[float] = []
        local_err = 0
        for _ in range(per_thread):
            try:
                ms, code = one(args.base_url, args.path)
                if code != 200:
                    local_err += 1
                local.append(ms)
            except Exception:  # noqa: BLE001
                local_err += 1
        with lock:
            latencies.extend(local)
            errors += local_err

    threads = [threading.Thread(target=worker) for _ in range(args.concurrency)]
    wall_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - wall_start

    total = len(latencies)
    if total == 0:
        print("[bench] 没有成功的请求")
        return 1

    ok = total - errors
    print(f"[bench] path={args.path}")
    print(f"[bench] 并发={args.concurrency}  请求={total}  成功={ok}  失败={errors}")
    print(f"[bench] 吞吐={total / wall:.1f} req/s  总耗时={wall:.2f}s")
    print(
        f"[bench] 延迟(ms) 均值={statistics.mean(latencies):.2f}  "
        f"P50={percentile(latencies, 50):.2f}  P95={percentile(latencies, 95):.2f}  "
        f"P99={percentile(latencies, 99):.2f}  max={max(latencies):.2f}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
