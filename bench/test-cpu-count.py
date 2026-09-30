#!/usr/bin/env python3
"""Compile the actual POSIX shim and exercise Linux affinity in a child process.

Run on Linux: python3 bench/test-cpu-count.py
Or cross-build: python3 bench/test-cpu-count.py --build-only /tmp/cpu-count-test --cc 'clang ...'
The produced executable can run under taskset or Docker --cpuset-cpus without special privileges.
"""
import argparse
import pathlib
import shlex
import subprocess
import tempfile


HARNESS = r'''
#include <assert.h>
#include <sys/wait.h>

static int fake_mode;
static int affinity_calls;
static int neton_test_getaffinity(pid_t pid, size_t bytes, cpu_set_t *mask) {
    ++affinity_calls;
    if (!fake_mode) return sched_getaffinity(pid, bytes, mask);
    if (fake_mode == 3) { errno = EPERM; return -1; }
    size_t needed = fake_mode == 2 ? 512 : 128;
    if (bytes < needed) { errno = EINVAL; return -1; }
    unsigned char *p = (unsigned char *) mask;
    memset(p, 0, bytes);
    int ids[] = {0, 31, 64, fake_mode == 2 ? 2048 : 95};
    for (int i = 0; i < 4; ++i) p[ids[i] / 8] |= (unsigned char) (1u << (ids[i] % 8));
    return 0;
}

int main(void) {
    cpu_set_t original, one, sparse;
    CPU_ZERO(&original);
    assert(sched_getaffinity(0, sizeof original, &original) == 0);
    int allowed = CPU_COUNT(&original);
    printf("online=%ld allowed=%d detected=%d\n", sysconf(_SC_NPROCESSORS_ONLN), allowed, neton_cpu_count());
    assert(allowed > 0 && neton_cpu_count() == allowed);
    int first = -1, last = -1;
    for (int i = 0; i < CPU_SETSIZE; ++i) if (CPU_ISSET(i, &original)) {
        if (first < 0) first = i;
        last = i;
    }
    CPU_ZERO(&one);
    CPU_SET(first, &one);
    assert(sched_setaffinity(0, sizeof one, &one) == 0);
    assert(neton_cpu_count() == 1);
    pid_t pid = fork();
    assert(pid >= 0);
    if (pid == 0) _exit(neton_cpu_count() == 1 ? 0 : 1);
    int status;
    assert(waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    CPU_ZERO(&sparse);
    CPU_SET(first, &sparse);
    CPU_SET(last, &sparse);
    assert(sched_setaffinity(0, sizeof sparse, &sparse) == 0);
    assert(neton_cpu_count() == (first == last ? 1 : 2));
    assert(sched_setaffinity(0, sizeof original, &original) == 0);
    assert(neton_cpu_count() == allowed);
    puts("PASS: original, restricted, inherited, first/last CPU mask, restored (no cached count)");
    fake_mode = 1;
    assert(neton_cpu_count() == 4);
    fake_mode = 2;
    affinity_calls = 0;
    assert(neton_cpu_count() == 4 && affinity_calls > 1);
    fake_mode = 3;
    long online = sysconf(_SC_NPROCESSORS_ONLN);
    assert(neton_cpu_count() == (online < 1 ? 1 : online));
    puts("PASS: injected sparse/high-index masks, EINVAL growth, syscall-failure fallback");
    return 0;
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cc", default="cc")
    parser.add_argument("--build-only", type=pathlib.Path)
    parser.add_argument("--source-ref", help="Build a historical shim as a negative control (does not change the worktree)")
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[1]
    shim_path = "neton-io/src/nativeInterop/cinterop/posixshim.def"
    source = (subprocess.check_output(["git", "show", args.source_ref + ":" + shim_path], cwd=root, text=True)
              if args.source_ref else (root / shim_path).read_text())
    shim = source.split("\n---\n", 1)[1]
    # Redirect only the shim's syscall; the harness delegates to the real kernel by default.
    shim = shim.replace("sched_getaffinity(", "neton_test_getaffinity(")
    prefix = "#include <sched.h>\n#include <unistd.h>\nstatic int neton_test_getaffinity(pid_t, size_t, cpu_set_t *);\n"
    with tempfile.TemporaryDirectory(prefix="neton-cpu-test-") as tmp:
        src = pathlib.Path(tmp) / "test.c"
        src.write_text(prefix + shim + HARNESS)
        binary = args.build_only.resolve() if args.build_only else pathlib.Path(tmp) / "test"
        subprocess.run(shlex.split(args.cc) + ["-D_GNU_SOURCE", "-std=c11", "-O2", "-Werror",
                       "-pthread", str(src), "-o", str(binary)], check=True)
        if not args.build_only:
            subprocess.run([str(binary)], check=True)


if __name__ == "__main__":
    main()
