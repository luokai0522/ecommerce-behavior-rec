"""按固定步长抽样天池 UserBehavior 数据集，保留时间跨度。

原始文件约 1 亿行、3.4GB，按顺序读取并每 K 行保留 1 行，
这样抽样结果在时间上均匀分布（数据本身按时间戳递增排列），
避免"只取前 N 行"导致全部落在第一天。

用法：
    python tools/sample_dataset.py data/raw/UserBehavior.csv data/raw/sample_2m.csv --rows 2000000
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="UserBehavior 数据集抽样")
    p.add_argument("src", help="原始 CSV 路径")
    p.add_argument("dst", help="输出 CSV 路径")
    p.add_argument("--rows", type=int, default=2_000_000, help="目标行数")
    p.add_argument(
        "--keep-every",
        type=int,
        default=0,
        help="每 K 行保留 1 行；0 表示按目标行数自动推算",
    )
    return p.parse_args(argv)


def estimate_lines(path: Path) -> int:
    """粗略估算总行数：按前 64MB 的平均行长推算。"""
    probe = 64 * 1024 * 1024
    with path.open("rb") as f:
        chunk = f.read(probe)
    lines = chunk.count(b"\n")
    if lines == 0:
        return 0
    avg = len(chunk) / lines
    return int(path.stat().st_size / avg)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    src, dst = Path(args.src), Path(args.dst)
    if not src.exists():
        print(f"[错误] 源文件不存在: {src}", file=sys.stderr)
        return 1

    total = estimate_lines(src)
    keep_every = args.keep_every
    if keep_every <= 0:
        keep_every = max(1, total // max(args.rows, 1))

    print(f"源文件: {src}（约 {total:,} 行，{src.stat().st_size / 1024**3:.2f} GB）")
    print(f"抽样策略: 每 {keep_every} 行保留 1 行，目标 {args.rows:,} 行")

    dst.parent.mkdir(parents=True, exist_ok=True)
    read = kept = 0
    first_ts = last_ts = None
    with src.open("r", encoding="utf-8", errors="ignore") as fin, dst.open(
        "w", encoding="utf-8", newline="\n"
    ) as fout:
        for line in fin:
            read += 1
            if read % keep_every:
                continue
            fout.write(line)
            parts = line.rstrip("\n").split(",")
            if len(parts) == 5:
                if first_ts is None:
                    first_ts = parts[4]
                last_ts = parts[4]
            kept += 1
            if kept >= args.rows:
                break

    print(f"完成: 读取 {read:,} 行，写出 {kept:,} 行 -> {dst}")
    print(f"时间戳范围: {first_ts} ~ {last_ts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
