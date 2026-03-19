#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import time
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TMP = ROOT / "tmp" / "mira-benchmarks"
WANDBOX_URL = "https://wandbox.org/api/compile.json"
USER_AGENT = "Mozilla/5.0 (Codex Raven MIRA Benchmark)"


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def extract_mira_prelude(path: Path) -> str:
    source = read_text(path)
    marker = "int main(void) {"
    index = source.find(marker)
    if index < 0:
        raise ValueError(f"benchmark source does not contain main: {path}")
    return source[:index].rstrip() + "\n\n"


CASES = {
    "sum_abs": {
        "iterations": 50,
        "unit_work": 20_000,
        "mira_c_file": ROOT / "tmp" / "mira-native" / "bench_sum_abs.c",
        "mira_main": r"""
int main(void) {
  static int32_t bench_xs_data[20000];
  for (int i = 0; i < 20000; ++i) {
    bench_xs_data[i] = (int32_t) (i - 10000);
  }
  span_i32 bench_xs = { .data = bench_xs_data, .len = ((uint32_t) 20000u) };
  volatile int64_t sink = 0;
  uint32_t bench_seed = 1u;
  for (int warm = 0; warm < 2; ++warm) {
    bench_seed = bench_seed * 1664525u + 1013904223u;
    if (bench_xs.len > 0u) { bench_xs_data[0] = ((int32_t) (((bench_seed >> 0) & 1023u) - 511)); }
    sink ^= (int64_t) mira_func_sum_abs(bench_xs);
  }
  uint64_t samples[5] = {0};
  for (int sample = 0; sample < 5; ++sample) {
    uint64_t started = mira_now_ns();
    for (uint32_t iter = 0; iter < 50u; ++iter) {
      bench_seed = bench_seed * 1664525u + 1013904223u;
      if (bench_xs.len > 0u) { bench_xs_data[0] = ((int32_t) (((bench_seed >> 0) & 1023u) - 511)); }
      sink ^= (int64_t) mira_func_sum_abs(bench_xs);
    }
    uint64_t ended = mira_now_ns();
    samples[sample] = ended - started;
  }
  fprintf(stdout, "SINK=%" PRId64 "\n", sink);
  for (int sample = 0; sample < 5; ++sample) {
    fprintf(stdout, "SAMPLE=%" PRIu64 "\n", samples[sample]);
  }
  return 0;
}
""".strip()
    },
    "dot_product": {
        "iterations": 40,
        "unit_work": 15_000,
        "mira_c_file": ROOT / "tmp" / "mira-native" / "bench_dot_product.c",
        "mira_main": r"""
int main(void) {
  static int32_t bench_xs_data[15000];
  static int32_t bench_ys_data[15000];
  for (int i = 0; i < 15000; ++i) {
    bench_xs_data[i] = (int32_t) i;
    bench_ys_data[i] = (int32_t) (i + 30000);
  }
  span_i32 bench_xs = { .data = bench_xs_data, .len = ((uint32_t) 15000u) };
  span_i32 bench_ys = { .data = bench_ys_data, .len = ((uint32_t) 15000u) };
  volatile int64_t sink = 0;
  uint32_t bench_seed = 1u;
  for (int warm = 0; warm < 2; ++warm) {
    bench_seed = bench_seed * 1664525u + 1013904223u;
    if (bench_xs.len > 0u) { bench_xs_data[0] = ((int32_t) (((bench_seed >> 0) & 1023u) - 511)); }
    if (bench_ys.len > 0u) { bench_ys_data[0] = ((int32_t) (((bench_seed >> 1) & 1023u) - 511)); }
    sink ^= (int64_t) mira_func_dot_product(bench_xs, bench_ys);
  }
  uint64_t samples[5] = {0};
  for (int sample = 0; sample < 5; ++sample) {
    uint64_t started = mira_now_ns();
    for (uint32_t iter = 0; iter < 40u; ++iter) {
      bench_seed = bench_seed * 1664525u + 1013904223u;
      if (bench_xs.len > 0u) { bench_xs_data[0] = ((int32_t) (((bench_seed >> 0) & 1023u) - 511)); }
      if (bench_ys.len > 0u) { bench_ys_data[0] = ((int32_t) (((bench_seed >> 1) & 1023u) - 511)); }
      sink ^= (int64_t) mira_func_dot_product(bench_xs, bench_ys);
    }
    uint64_t ended = mira_now_ns();
    samples[sample] = ended - started;
  }
  fprintf(stdout, "SINK=%" PRId64 "\n", sink);
  for (int sample = 0; sample < 5; ++sample) {
    fprintf(stdout, "SAMPLE=%" PRIu64 "\n", samples[sample]);
  }
  return 0;
}
""".strip()
    },
    "fib_iter": {
        "iterations": 100_000,
        "unit_work": 60,
        "mira_c_file": ROOT / "tmp" / "mira-native" / "bench_fib_iter.c",
        "mira_main": r"""
int main(void) {
  int32_t bench_n = ((int32_t) 60);
  volatile int64_t sink = 0;
  uint32_t bench_seed = 1u;
  for (int warm = 0; warm < 2; ++warm) {
    bench_seed = bench_seed * 1664525u + 1013904223u;
    bench_n = ((int32_t) (40 + ((bench_seed >> 0) & 15u)));
    sink ^= (int64_t) mira_func_fib_iter(bench_n);
  }
  uint64_t samples[5] = {0};
  for (int sample = 0; sample < 5; ++sample) {
    uint64_t started = mira_now_ns();
    for (uint32_t iter = 0; iter < 100000u; ++iter) {
      bench_seed = bench_seed * 1664525u + 1013904223u;
      bench_n = ((int32_t) (40 + ((bench_seed >> 0) & 15u)));
      sink ^= (int64_t) mira_func_fib_iter(bench_n);
    }
    uint64_t ended = mira_now_ns();
    samples[sample] = ended - started;
  }
  fprintf(stdout, "SINK=%" PRId64 "\n", sink);
  for (int sample = 0; sample < 5; ++sample) {
    fprintf(stdout, "SAMPLE=%" PRIu64 "\n", samples[sample]);
  }
  return 0;
}
""".strip()
    },
}


def manual_c_code(case: str) -> str:
    if case == "sum_abs":
        body = r"""
static __attribute__((noinline)) int64_t manual_sum_abs(const int32_t* xs, uint32_t len) {
  int64_t acc = 0;
  for (uint32_t i = 0; i < len; ++i) {
    int32_t value = xs[i];
    acc += (int64_t) (value < 0 ? -value : value);
  }
  return acc;
}
"""
        main = CASES[case]["mira_main"].replace("mira_func_sum_abs(bench_xs)", "manual_sum_abs(bench_xs_data, bench_xs.len)")
    elif case == "dot_product":
        body = r"""
static __attribute__((noinline)) int64_t manual_dot_product(const int32_t* xs, const int32_t* ys, uint32_t len) {
  int64_t acc = 0;
  for (uint32_t i = 0; i < len; ++i) {
    acc += (int64_t) (xs[i] * ys[i]);
  }
  return acc;
}
"""
        main = CASES[case]["mira_main"].replace(
            "mira_func_dot_product(bench_xs, bench_ys)",
            "manual_dot_product(bench_xs_data, bench_ys_data, bench_xs.len)",
        )
    else:
        body = r"""
static __attribute__((noinline)) int64_t manual_fib_iter(int32_t n) {
  if (n <= 1) { return (int64_t) n; }
  int64_t a = 0;
  int64_t b = 1;
  for (int32_t i = 2; i <= n; ++i) {
    int64_t next = a + b;
    a = b;
    b = next;
  }
  return b;
}
"""
        main = CASES[case]["mira_main"].replace("mira_func_fib_iter(bench_n)", "manual_fib_iter(bench_n)")
    return (
        "#include <inttypes.h>\n"
        "#include <stdbool.h>\n"
        "#include <stdint.h>\n"
        "#include <stdio.h>\n"
        "#include <time.h>\n\n"
        "typedef struct { const int32_t* data; uint32_t len; } span_i32;\n\n"
        "static uint64_t mira_now_ns(void) {\n"
        "  struct timespec ts;\n"
        "  clock_gettime(CLOCK_MONOTONIC, &ts);\n"
        "  return ((uint64_t) ts.tv_sec * 1000000000ULL) + (uint64_t) ts.tv_nsec;\n"
        "}\n\n"
        + body.strip()
        + "\n\n"
        + main
        + "\n"
    )


def rust_code(case: str) -> str:
    common = """
use std::time::Instant;

fn print_samples(sink: i64, samples: &[u64]) {
    println!("SINK={sink}");
    for sample in samples {
        println!("SAMPLE={sample}");
    }
}
"""
    if case == "sum_abs":
        return common + """
#[inline(never)]
fn sum_abs(xs: &[i32]) -> i64 {
    let mut acc = 0i64;
    for &value in xs {
        acc += i64::from(value.abs());
    }
    acc
}

fn main() {
    let mut xs: Vec<i32> = (-10000..10000).collect();
    let mut sink = 0i64;
    let mut bench_seed = 1u32;
    for _ in 0..2 {
        bench_seed = bench_seed.wrapping_mul(1664525).wrapping_add(1013904223);
        xs[0] = (((bench_seed >> 0) & 1023) as i32) - 511;
        sink ^= sum_abs(&xs);
    }
    let mut samples = [0u64; 5];
    for sample in &mut samples {
        let started = Instant::now();
        for _ in 0..50u32 {
            bench_seed = bench_seed.wrapping_mul(1664525).wrapping_add(1013904223);
            xs[0] = (((bench_seed >> 0) & 1023) as i32) - 511;
            sink ^= sum_abs(&xs);
        }
        *sample = started.elapsed().as_nanos() as u64;
    }
    print_samples(sink, &samples);
}
"""
    if case == "dot_product":
        return common + """
#[inline(never)]
fn dot_product(xs: &[i32], ys: &[i32]) -> i64 {
    if xs.len() != ys.len() {
        return 0;
    }
    let mut acc = 0i64;
    for (&x, &y) in xs.iter().zip(ys.iter()) {
        acc += i64::from(x * y);
    }
    acc
}

fn main() {
    let mut xs: Vec<i32> = (0..15000).collect();
    let mut ys: Vec<i32> = (30000..45000).collect();
    let mut sink = 0i64;
    let mut bench_seed = 1u32;
    for _ in 0..2 {
        bench_seed = bench_seed.wrapping_mul(1664525).wrapping_add(1013904223);
        xs[0] = (((bench_seed >> 0) & 1023) as i32) - 511;
        ys[0] = (((bench_seed >> 1) & 1023) as i32) - 511;
        sink ^= dot_product(&xs, &ys);
    }
    let mut samples = [0u64; 5];
    for sample in &mut samples {
        let started = Instant::now();
        for _ in 0..40u32 {
            bench_seed = bench_seed.wrapping_mul(1664525).wrapping_add(1013904223);
            xs[0] = (((bench_seed >> 0) & 1023) as i32) - 511;
            ys[0] = (((bench_seed >> 1) & 1023) as i32) - 511;
            sink ^= dot_product(&xs, &ys);
        }
        *sample = started.elapsed().as_nanos() as u64;
    }
    print_samples(sink, &samples);
}
"""
    return common + """
#[inline(never)]
fn fib_iter(n: i32) -> i64 {
    if n <= 1 {
        return i64::from(n);
    }
    let mut a = 0i64;
    let mut b = 1i64;
    let mut i = 2i32;
    while i <= n {
        let next = a + b;
        a = b;
        b = next;
        i += 1;
    }
    b
}

fn main() {
    let mut bench_n = 60i32;
    let mut sink = 0i64;
    let mut bench_seed = 1u32;
    for _ in 0..2 {
        bench_seed = bench_seed.wrapping_mul(1664525).wrapping_add(1013904223);
        bench_n = 40 + ((bench_seed >> 0) & 15) as i32;
        sink ^= fib_iter(bench_n);
    }
    let mut samples = [0u64; 5];
    for sample in &mut samples {
        let started = Instant::now();
        for _ in 0..100000u32 {
            bench_seed = bench_seed.wrapping_mul(1664525).wrapping_add(1013904223);
            bench_n = 40 + ((bench_seed >> 0) & 15) as i32;
            sink ^= fib_iter(bench_n);
        }
        *sample = started.elapsed().as_nanos() as u64;
    }
    print_samples(sink, &samples);
}
"""


def go_code(case: str) -> str:
    if case == "sum_abs":
        body = """
func sumAbs(xs []int32) int64 {
    var acc int64
    for _, value := range xs {
        if value < 0 {
            acc += int64(-value)
        } else {
            acc += int64(value)
        }
    }
    return acc
}

func main() {
    xs := make([]int32, 20000)
    for i := range xs {
        xs[i] = int32(i - 10000)
    }
    var sink int64
    benchSeed := uint32(1)
    for warm := 0; warm < 2; warm++ {
        benchSeed = benchSeed*1664525 + 1013904223
        xs[0] = int32((benchSeed&1023)) - 511
        sink ^= sumAbs(xs)
    }
    samples := make([]uint64, 5)
    for sample := range samples {
        started := time.Now()
        for iter := 0; iter < 50; iter++ {
            benchSeed = benchSeed*1664525 + 1013904223
            xs[0] = int32((benchSeed&1023)) - 511
            sink ^= sumAbs(xs)
        }
        samples[sample] = uint64(time.Since(started).Nanoseconds())
    }
    fmt.Printf("SINK=%d\\n", sink)
    for _, sample := range samples {
        fmt.Printf("SAMPLE=%d\\n", sample)
    }
}
"""
    elif case == "dot_product":
        body = """
func dotProduct(xs []int32, ys []int32) int64 {
    if len(xs) != len(ys) {
        return 0
    }
    var acc int64
    for i, x := range xs {
        acc += int64(x * ys[i])
    }
    return acc
}

func main() {
    xs := make([]int32, 15000)
    ys := make([]int32, 15000)
    for i := range xs {
        xs[i] = int32(i)
        ys[i] = int32(i + 30000)
    }
    var sink int64
    benchSeed := uint32(1)
    for warm := 0; warm < 2; warm++ {
        benchSeed = benchSeed*1664525 + 1013904223
        xs[0] = int32((benchSeed&1023)) - 511
        ys[0] = int32(((benchSeed >> 1) & 1023)) - 511
        sink ^= dotProduct(xs, ys)
    }
    samples := make([]uint64, 5)
    for sample := range samples {
        started := time.Now()
        for iter := 0; iter < 40; iter++ {
            benchSeed = benchSeed*1664525 + 1013904223
            xs[0] = int32((benchSeed&1023)) - 511
            ys[0] = int32(((benchSeed >> 1) & 1023)) - 511
            sink ^= dotProduct(xs, ys)
        }
        samples[sample] = uint64(time.Since(started).Nanoseconds())
    }
    fmt.Printf("SINK=%d\\n", sink)
    for _, sample := range samples {
        fmt.Printf("SAMPLE=%d\\n", sample)
    }
}
"""
    else:
        body = """
func fibIter(n int32) int64 {
    if n <= 1 {
        return int64(n)
    }
    var a int64 = 0
    var b int64 = 1
    for i := int32(2); i <= n; i++ {
        next := a + b
        a = b
        b = next
    }
    return b
}

func main() {
    benchN := int32(60)
    var sink int64
    benchSeed := uint32(1)
    for warm := 0; warm < 2; warm++ {
        benchSeed = benchSeed*1664525 + 1013904223
        benchN = int32(40 + ((benchSeed >> 0) & 15))
        sink ^= fibIter(benchN)
    }
    samples := make([]uint64, 5)
    for sample := range samples {
        started := time.Now()
        for iter := 0; iter < 100000; iter++ {
            benchSeed = benchSeed*1664525 + 1013904223
            benchN = int32(40 + ((benchSeed >> 0) & 15))
            sink ^= fibIter(benchN)
        }
        samples[sample] = uint64(time.Since(started).Nanoseconds())
    }
    fmt.Printf("SINK=%d\\n", sink)
    for _, sample := range samples {
        fmt.Printf("SAMPLE=%d\\n", sample)
    }
}
"""
    return "package main\n\nimport (\n    \"fmt\"\n    \"time\"\n)\n\n" + body


def python_code(case: str) -> str:
    if case == "sum_abs":
        body = """
def sum_abs(xs):
    acc = 0
    for value in xs:
        acc += -value if value < 0 else value
    return acc

xs = list(range(-10000, 10000))
sink = 0
bench_seed = 1
for _ in range(2):
    bench_seed = (bench_seed * 1664525 + 1013904223) & 0xFFFFFFFF
    xs[0] = ((bench_seed >> 0) & 1023) - 511
    sink ^= sum_abs(xs)
samples = []
for _ in range(5):
    started = time.perf_counter_ns()
    for _ in range(50):
        bench_seed = (bench_seed * 1664525 + 1013904223) & 0xFFFFFFFF
        xs[0] = ((bench_seed >> 0) & 1023) - 511
        sink ^= sum_abs(xs)
    samples.append(time.perf_counter_ns() - started)
"""
    elif case == "dot_product":
        body = """
def dot_product(xs, ys):
    if len(xs) != len(ys):
        return 0
    acc = 0
    for x, y in zip(xs, ys):
        acc += x * y
    return acc

xs = list(range(15000))
ys = list(range(30000, 45000))
sink = 0
bench_seed = 1
for _ in range(2):
    bench_seed = (bench_seed * 1664525 + 1013904223) & 0xFFFFFFFF
    xs[0] = ((bench_seed >> 0) & 1023) - 511
    ys[0] = ((bench_seed >> 1) & 1023) - 511
    sink ^= dot_product(xs, ys)
samples = []
for _ in range(5):
    started = time.perf_counter_ns()
    for _ in range(40):
        bench_seed = (bench_seed * 1664525 + 1013904223) & 0xFFFFFFFF
        xs[0] = ((bench_seed >> 0) & 1023) - 511
        ys[0] = ((bench_seed >> 1) & 1023) - 511
        sink ^= dot_product(xs, ys)
    samples.append(time.perf_counter_ns() - started)
"""
    else:
        body = """
def fib_iter(n):
    if n <= 1:
        return n
    a = 0
    b = 1
    i = 2
    while i <= n:
        a, b = b, a + b
        i += 1
    return b

bench_n = 60
sink = 0
bench_seed = 1
for _ in range(2):
    bench_seed = (bench_seed * 1664525 + 1013904223) & 0xFFFFFFFF
    bench_n = 40 + ((bench_seed >> 0) & 15)
    sink ^= fib_iter(bench_n)
samples = []
for _ in range(5):
    started = time.perf_counter_ns()
    for _ in range(100000):
        bench_seed = (bench_seed * 1664525 + 1013904223) & 0xFFFFFFFF
        bench_n = 40 + ((bench_seed >> 0) & 15)
        sink ^= fib_iter(bench_n)
    samples.append(time.perf_counter_ns() - started)
"""
    return (
        "import time\n\n"
        + body.strip()
        + "\nprint(f'SINK={sink}')\nfor sample in samples:\n    print(f'SAMPLE={sample}')\n"
    )


def build_sources(case: str) -> dict[str, tuple[str, dict[str, str]]]:
    prelude = extract_mira_prelude(CASES[case]["mira_c_file"])
    return {
        "mira_c": (
            prelude + CASES[case]["mira_main"] + "\n",
            {"compiler": "gcc-13.2.0-c", "options": "warning,optimize"},
        ),
        "c": (
            manual_c_code(case),
            {"compiler": "gcc-13.2.0-c", "options": "warning,optimize"},
        ),
        "rust": (
            rust_code(case),
            {
                "compiler": "rust-1.82.0",
                "compiler-option-raw": "-O",
            },
        ),
        "go": (
            go_code(case),
            {"compiler": "go-1.23.2"},
        ),
        "python": (
            python_code(case),
            {"compiler": "cpython-3.13.8"},
        ),
    }


def submit_wandbox(code: str, config: dict[str, str]) -> dict:
    payload = {"code": code, "save": False}
    payload.update(config)
    request = urllib.request.Request(
        WANDBOX_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_samples(output: str) -> list[int]:
    samples = []
    for line in output.splitlines():
        if line.startswith("SAMPLE="):
            samples.append(int(line.split("=", 1)[1]))
    if not samples:
        raise ValueError(f"benchmark output contains no samples:\n{output}")
    return samples


def median(samples: list[int]) -> int:
    return int(statistics.median(sorted(samples)))


def run_suite(delay_sec: float) -> dict:
    suite = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "service": "wandbox", "results": []}
    for case_name, case in CASES.items():
        sources = build_sources(case_name)
        for language, (code, config) in sources.items():
            response = submit_wandbox(code, config)
            if response.get("status") != "0":
                raise RuntimeError(
                    f"wandbox failed for {case_name}/{language}: "
                    f"{response.get('compiler_error') or response.get('program_error') or response}"
                )
            stdout = response.get("program_output", "") or ""
            samples = parse_samples(stdout)
            median_ns = median(samples)
            suite["results"].append(
                {
                    "case": case_name,
                    "language": language,
                    "compiler": config["compiler"],
                    "samples_ns": samples,
                    "median_ns": median_ns,
                    "per_call_ns": median_ns / case["iterations"],
                    "units_per_second": (case["iterations"] * case["unit_work"]) / (median_ns / 1_000_000_000.0),
                }
            )
            time.sleep(delay_sec)
    return suite


def render_markdown(results: dict) -> str:
    lines = ["# Remote MIRA Benchmarks", "", f"Service: `{results['service']}`", f"Generated: `{results['generated_at']}`", ""]
    grouped: dict[str, list[dict]] = {}
    for item in results["results"]:
        grouped.setdefault(item["case"], []).append(item)
    for case_name, items in grouped.items():
        items.sort(key=lambda item: item["median_ns"])
        baseline = next(item for item in items if item["language"] == "c")
        lines.append(f"## {case_name}")
        lines.append("")
        lines.append("| language | compiler | median_ns | per_call_ns | units_per_second | vs_c |")
        lines.append("| --- | --- | ---: | ---: | ---: | ---: |")
        for item in items:
            ratio = item["median_ns"] / baseline["median_ns"]
            lines.append(
                f"| {item['language']} | {item['compiler']} | {item['median_ns']} | "
                f"{item['per_call_ns']:.3f} | {item['units_per_second']:.3f} | {ratio:.3f}x |"
            )
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run remote MIRA benchmarks on Wandbox.")
    parser.add_argument(
        "--output-json",
        default=str(TMP / "remote-latest.json"),
        help="Path to write JSON benchmark results.",
    )
    parser.add_argument(
        "--output-md",
        default=str(TMP / "remote-latest.md"),
        help="Path to write markdown benchmark summary.",
    )
    parser.add_argument(
        "--delay-sec",
        type=float,
        default=0.6,
        help="Delay between remote submissions.",
    )
    args = parser.parse_args()
    TMP.mkdir(parents=True, exist_ok=True)
    results = run_suite(args.delay_sec)
    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.write_text(json.dumps(results, indent=2), encoding="utf-8")
    output_md.write_text(render_markdown(results), encoding="utf-8")
    print(json.dumps({"ok": True, "json": str(output_json), "md": str(output_md)}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
