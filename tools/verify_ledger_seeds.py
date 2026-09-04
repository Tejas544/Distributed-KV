#!/usr/bin/env python3
"""P8 exit criterion 4: every ledger bug's seed reproduces before its fix and
passes after -- for the rows where that question is even answerable.

    tools/verify_ledger_seeds.py

Of BUGS.md's ~66 rows, the great majority were fixed inside one of five bulk
phase commits (P2 through P6) landed before this project started giving each
bug its own commit. `fix_commit: P5` names a *phase*, not a commit -- there is
no single parent/child pair to bisect, and synthesizing one would mean
rewriting already-published history to invent a granularity that was never
there. That is not on the table (CLAUDE.md's git safety rules, and the ledger's
own `commit_found` field would start lying the moment the history under it
changed).

So this tool does the honest version: for every row whose `fix_commit` is a
real, resolvable git commit, it checks out that commit's parent (`git worktree
add`, never touching the working tree this script runs from) and the commit
itself, builds the suite the row's corpus file names, and asserts the seed
reproduces on the parent and is clean on the fix. Every other row is reported
as unbisectable, with the phase label that is the real reason, which is itself
the correct P8 answer for that row -- a documented gap, not a faked pass.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# MSYS2/Git-Bash's /tmp and CPython's own idea of a temp dir are not the same
# directory on Windows (tools/mutation_report.py hits the identical gotcha) --
# the shell maps /tmp to %TEMP%, CPython does not. Use CPython's own resolver
# so paths this script builds and paths it later hands to subprocess.run(cwd=)
# actually agree.
TMP = tempfile.gettempdir()
WORKTREE_BASE = os.path.join(TMP, "anvil_verify_ledger_wt")

_YAML_LINE = re.compile(r"^([a-z_]+):\s*(.*)$")
_SHA_LIKE = re.compile(r"^[0-9a-f]{7,40}$")
_REPRODUCE = re.compile(r"^\s*reproduce\s*=\s*(\S+)\s+(\d+)\s*$", re.M)
_INVARIANT = re.compile(r"^\s*invariant\s*=\s*(.+?)\s*$", re.M)


def parse_bugs_md(path):
    """Yields dicts with at least id/fix_commit/regression for each YAML block."""
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    for block in re.findall(r"```yaml\n(.*?)\n```", text, flags=re.S):
        row = {}
        for line in block.splitlines():
            m = _YAML_LINE.match(line)
            if m:
                row[m.group(1)] = m.group(2).strip()
        if "id" in row:
            yield row


def resolve_commit(sha):
    """Returns the full SHA if it exists in this repo, else None."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--verify", f"{sha}^{{commit}}"],
            cwd=ROOT, capture_output=True, text=True, check=True,
        )
        return out.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def parse_corpus_spec(regression_field):
    """From `regression: test/corpus/ANV-XXXX.seed (...)` get the seed file,
    then from that file's `reproduce = <suite> <n>` line get what to run."""
    m = re.match(r"([\w/.\-]+\.seed)", regression_field)
    if not m:
        return None
    seed_path = os.path.join(ROOT, m.group(1))
    if not os.path.isfile(seed_path):
        return None
    with open(seed_path, encoding="utf-8") as fh:
        text = fh.read()
    rm = _REPRODUCE.search(text)
    if not rm:
        return None
    suite, count = rm.group(1), rm.group(2)
    # .seed files write the binary name (`anvil_shard_faults`); build.sh and
    # the ANVIL_BIN layout both key off the bare suite name (`shard_faults`).
    if suite.startswith("anvil_"):
        suite = suite[len("anvil_"):]
    im = _INVARIANT.search(text)
    invariant = im.group(1) if im else ""
    return {"suite": suite, "count": count, "invariant": invariant, "seed_file": m.group(1)}


_GXX_EXE = shutil.which("g++")
# Same recipe tools/build.sh uses, replicated directly rather than shelled out
# to -- tools/build.sh itself did not exist before the P7 commit (9572369),
# and every one of the commits this tool bisects predates it. The question
# this tool answers is "does the source at this commit reproduce the bug",
# not "did this historical revision's own build tooling work", so one modern
# compile recipe applied uniformly to whatever source is checked out is the
# more correct choice, not just the more convenient one.
_GXX_FLAGS = ["-std=c++20", "-DANVIL_ENABLE_BUGGIFY=1", "-O2",
              "-fno-threadsafe-statics", "-ffp-contract=off", "-fwrapv"]


def _compile_worktree(wt, obj_dir, log):
    os.makedirs(obj_dir, exist_ok=True)
    objs = []
    for dirpath, _, filenames in os.walk(wt):
        for name in sorted(filenames):
            if not name.endswith(".cc") or name == "sim_main.cc":
                continue
            rel = os.path.relpath(os.path.join(dirpath, name), wt)
            top = rel.split(os.sep, 1)[0]
            if top not in ("anvil", "workloads"):
                continue
            mangled = rel.replace(os.sep, "_").replace("/", "_")[:-3] + ".o"
            o = os.path.join(obj_dir, mangled)
            objs.append(o)
            if os.path.exists(o):
                continue
            r = subprocess.run(
                [_GXX_EXE, *_GXX_FLAGS, "-I", wt, "-c", os.path.join(wt, rel), "-o", o],
                capture_output=True, text=True,
            )
            if r.returncode != 0:
                return None, f"compiling {rel}:\n{r.stderr[-2000:]}"
    return objs, None


def build_and_run(commit, suite_counts, log):
    """git worktree at `commit`, build each suite, run it with the seed count
    its corpus file names, return {suite: (exit_code, output)}. Isolated
    object/bin caches per commit so stale objects from one checkout never
    leak into another's build."""
    wt = os.path.join(WORKTREE_BASE, commit[:12])
    shutil.rmtree(wt, ignore_errors=True)
    subprocess.run(["git", "worktree", "add", "--detach", wt, commit],
                    cwd=ROOT, check=True, capture_output=True, text=True)
    obj_dir = os.path.join(TMP, f"anvilobj-verify-{commit[:12]}")
    bin_dir = os.path.join(TMP, f"anvilbin-verify-{commit[:12]}")
    try:
        log(f"    compiling @ {commit[:12]} ...")
        objs, err = _compile_worktree(wt, obj_dir, log)
        if err:
            log(f"    BUILD FAILED @ {commit[:12]}: {err}")
            return {s: (None, "") for s in suite_counts}
        os.makedirs(bin_dir, exist_ok=True)
        results = {}
        for suite, count in suite_counts.items():
            src = os.path.join(wt, "test", f"{suite}.cc")
            if not os.path.isfile(src):
                log(f"    test/{suite}.cc does not exist @ {commit[:12]}")
                results[suite] = (None, "")
                continue
            exe = os.path.join(bin_dir, f"anvil_{suite}.exe")
            link = subprocess.run(
                [_GXX_EXE, *_GXX_FLAGS, "-I", wt, src, *objs, "-o", exe],
                capture_output=True, text=True,
            )
            if link.returncode != 0:
                log(f"    LINK FAILED for {suite} @ {commit[:12]}:\n{link.stderr[-2000:]}")
                results[suite] = (None, "")
                continue
            # The seed count the corpus file names -- not the binary's own
            # default -- because the seed a row pins is only reached if the
            # sweep runs at least that far. Passing no argument at all was the
            # first version of this and it silently ran every suite at its
            # default count instead, which never reproduced anything.
            run = subprocess.run([exe, str(count)], capture_output=True, text=True)
            results[suite] = (run.returncode, run.stdout + run.stderr)
        return results
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", wt],
                        cwd=ROOT, capture_output=True, text=True)
        shutil.rmtree(obj_dir, ignore_errors=True)
        shutil.rmtree(bin_dir, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep-going", action="store_true",
                     help="don't stop the whole run if one commit fails to build")
    args = ap.parse_args()

    rows = list(parse_bugs_md(os.path.join(ROOT, "BUGS.md")))

    bisectable = []   # (row, spec)
    unbisectable = []  # (row, reason)
    for row in rows:
        fix = row.get("fix_commit", "")
        sha = resolve_commit(fix) if _SHA_LIKE.match(fix.split()[0] if fix.split() else "") else None
        if sha is None:
            reason = f"fix_commit is not a resolvable commit ({fix!r}) -- almost certainly a " \
                      "phase label, meaning this bug's fix was squashed into a bulk phase " \
                      "commit with dozens of others and there is no per-bug commit to bisect"
            unbisectable.append((row, reason))
            continue
        spec = parse_corpus_spec(row.get("regression", ""))
        if spec is None:
            unbisectable.append((row, "fix_commit is real but its corpus file has no parseable "
                                       "`reproduce = <suite> <n>` line"))
            continue
        spec["fix_sha"] = sha
        bisectable.append((row, spec))

    print("=" * 78)
    print("LEDGER SEED REGRESSION CHECK  (P8 exit criterion 4)")
    print("=" * 78)
    print(f"\n{len(rows)} ledger rows: {len(bisectable)} bisectable, "
          f"{len(unbisectable)} not (fix squashed into a phase commit)\n")

    # Group by fix commit: several rows can share one commit (e.g. ANV-0052
    # and ANV-0053 were fixed together), and several suites can be needed for
    # one commit, so build each commit once and run every suite it needs.
    by_commit = {}
    for row, spec in bisectable:
        by_commit.setdefault(spec["fix_sha"], []).append((row, spec))

    results = []  # (row, spec, before_rc, after_rc, ok)
    for fix_sha, group in by_commit.items():
        suite_counts = {spec["suite"]: spec["count"] for _, spec in group}
        parent = subprocess.run(["git", "rev-parse", f"{fix_sha}^"], cwd=ROOT,
                                 capture_output=True, text=True, check=True).stdout.strip()
        print(f"  {fix_sha[:12]} (parent {parent[:12]}) -- suites: "
              f"{', '.join(f'{s} {c}' for s, c in sorted(suite_counts.items()))}")
        before = build_and_run(parent, suite_counts, print)
        after = build_and_run(fix_sha, suite_counts, print)
        for row, spec in group:
            suite = spec["suite"]
            before_rc, before_out = before.get(suite, (None, ""))
            after_rc, after_out = after.get(suite, (None, ""))
            reproduced = before_rc is not None and before_rc != 0
            fixed_clean = after_rc is not None and after_rc == 0
            ok = reproduced and fixed_clean
            results.append((row, spec, before_rc, after_rc, ok))

    print()
    print("-" * 78)
    print("RESULTS")
    print("-" * 78)
    for row, spec, before_rc, after_rc, ok in sorted(results, key=lambda r: r[0]["id"]):
        verdict = "CONFIRMED" if ok else "MISMATCH"
        print(f"\n  {row['id']}  [{verdict}]")
        print(f"    suite ......... anvil_{spec['suite']} {spec['count']}")
        print(f"    invariant ..... {spec['invariant']}")
        print(f"    fix commit .... {spec['fix_sha'][:12]}")
        print(f"    parent (before) exit={before_rc}  "
              f"{'reproduces (nonzero exit, as expected)' if before_rc else 'DID NOT REPRODUCE'}")
        print(f"    fix (after)     exit={after_rc}  "
              f"{'clean (as expected)' if after_rc == 0 else 'STILL FAILING'}")

    print()
    print("-" * 78)
    print(f"UNBISECTABLE ({len(unbisectable)} rows -- fix predates per-bug commits)")
    print("-" * 78)
    for row, reason in sorted(unbisectable, key=lambda r: r[0]["id"]):
        print(f"  {row['id']}: {reason}")

    confirmed = sum(1 for *_, ok in results if ok)
    print()
    print(f"{confirmed}/{len(results)} bisectable rows confirmed fail-before/pass-after; "
          f"{len(unbisectable)} rows structurally unbisectable and documented as such.")
    return 0 if confirmed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
