#!/usr/bin/env python3
"""P8 exit criterion 6: every phase's seeded-mutation drill, as one report.

    tools/mutation_report.py                 # runs the suites
    tools/mutation_report.py --from out.txt  # re-reads a saved run

Each fault suite keeps its own drill, its own harness and its own human-readable
table -- moving them into one binary would mean six harnesses reimplemented in a
seventh, which is how a report ends up measuring something other than what
ships. Each row additionally prints one machine-readable `DRILL|` line
(test/drill_report.h); this merges them.

The column that matters is **API visibility**, and it is the reason this report
exists rather than a detection percentage. Every `no` is a bug class that an
outside-in checker -- Jepsen, a client-side history checker, anything that
observes the system through its API -- structurally could not have found. That
number is the entire empirical case for protocol-aware simulation testing, and
it is worth more than the detection rate beside it.
"""

import argparse
import collections
import os
import subprocess
import sys

def _bin_dir():
    """Where tools/build.sh put the binaries.

    MSYS2's /tmp and Python's /tmp are not the same directory on Windows -- the
    shell maps /tmp to %TEMP% and CPython does not -- so a hard-coded "/tmp/..."
    resolves for the build script and not for this. Cheaper to try both than to
    require every caller to set ANVIL_BIN.
    """
    explicit = os.environ.get("ANVIL_BIN")
    if explicit:
        return explicit
    for candidate in ("/tmp/anvilbin",
                      os.path.join(os.environ.get("TEMP", "/tmp"), "anvilbin"),
                      os.path.join(os.environ.get("TMP", "/tmp"), "anvilbin")):
        if os.path.isdir(candidate):
            return candidate
    return "/tmp/anvilbin"


BIN = _bin_dir()


def _exe(path):
    return path + ".exe" if os.path.exists(path + ".exe") else path

# Each suite, and the argument that keeps the drill meaningful without making
# the report take an hour. These are the seed counts the drills themselves use.
SUITES = [
    ("P3  raft", f"{BIN}/anvil_raft_faults", ["12"]),
    ("P4  mvcc", f"{BIN}/anvil_mvcc_faults", ["20"]),
    ("P5  shard", f"{BIN}/anvil_shard_faults", ["20"]),
    ("P6  txn", f"{BIN}/anvil_txn_faults", ["30"]),
]


def collect(text):
    rows = []
    for line in text.splitlines():
        if not line.startswith("DRILL|"):
            continue
        parts = line.split("|")
        if len(parts) != 11:
            continue
        _, phase, workload, name, detected, runs, by_inv, api, mttd, fired, kind = parts
        rows.append(
            {
                "phase": phase,
                "workload": workload,
                "name": name,
                "detected": int(detected),
                "runs": int(runs),
                "by_invariant": int(by_inv),
                "api_visible": int(api),
                "mttd_ms": int(mttd),
                "fired": fired,
                "kind": kind,
            }
        )
    return rows


def run_suites():
    text = []
    for label, binary, args in SUITES:
        if not os.path.exists(binary) and not os.path.exists(binary + ".exe"):
            print(f"  {label:<12} SKIPPED -- {binary} not built", file=sys.stderr)
            continue
        print(f"  {label:<12} running {os.path.basename(binary)} {' '.join(args)} ...",
              file=sys.stderr, flush=True)
        # A suite that fails its own assertions still emits its drill rows, and
        # the rows are the point here -- raft.faults is *expected* to be red on
        # seed 14 (ANV-0032), so a non-zero exit is not a reason to discard it.
        proc = subprocess.run([_exe(binary)] + args, capture_output=True, text=True)
        text.append(proc.stdout)
    return "\n".join(text)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="src", help="read a saved run instead of running the suites")
    args = ap.parse_args()

    text = open(args.src).read() if args.src else run_suites()
    rows = collect(text)
    if not rows:
        print("no DRILL| lines found. Build the suites first: "
              "tools/build.sh raft_faults mvcc_faults shard_faults txn_faults", file=sys.stderr)
        return 2

    print()
    print("=" * 108)
    print("SEEDED-MUTATION REPORT -- every deliberate bug in the tree, one table")
    print("=" * 108)
    print()
    print(f"  {'phase':<5} {'mutation':<34} {'kind':<12} {'detected':>10} "
          f"{'by invariant':>13} {'API-visible':>13}  invariants that fired")
    print("  " + "-" * 106)

    tally = collections.Counter()
    api_blind = []
    for r in sorted(rows, key=lambda r: (r["phase"], r["name"])):
        rate = f"{r['detected']}/{r['runs']}"
        inv = f"{r['by_invariant']}/{r['runs']}"
        api = f"{r['api_visible']}/{r['runs']}" if r["api_visible"] else "no"
        print(f"  {r['phase']:<5} {r['name'][:34]:<34} {r['kind']:<12} {rate:>10} "
              f"{inv:>13} {api:>13}  {r['fired'][:40]}")
        tally[r["kind"]] += 1
        if r["kind"] == "must-detect":
            tally["must-detect-total"] += 1
            if r["detected"] > 0:
                tally["must-detect-caught"] += 1
            if r["api_visible"] == 0 and r["detected"] > 0:
                api_blind.append(r)
        elif r["kind"] in ("control", "equivalent") and r["detected"] > 0:
            tally["misbehaving-controls"] += 1

    caught = tally["must-detect-caught"]
    total = tally["must-detect-total"]
    print()
    print(f"  must-detect .......... {caught}/{total} caught")
    print(f"  controls ............. {tally['control']} (must stay silent)")
    print(f"  equivalent-in-class .. {tally['equivalent']} (each with a written argument "
          f"in its suite)")
    print(f"  covered elsewhere .... {tally['covered']} (a named test is the detector of record)")
    if tally["misbehaving-controls"]:
        print(f"  *** {tally['misbehaving-controls']} control or equivalent row(s) FIRED. A firing "
              f"control means the harness is attributing something to the mutation.")

    print()
    print("-" * 108)
    print("THE API-VISIBILITY COLUMN")
    print("-" * 108)
    print()
    print("  Bugs that were caught, and that NO client could have observed. Each of these is a")
    print("  defect an outside-in checker structurally cannot find -- it corrupted internal")
    print("  state and was masked, garbage-collected or overwritten before any read could see")
    print("  it. This list is the empirical case for protocol-aware simulation testing.")
    print()
    if not api_blind:
        print("    (none in this run -- which is itself worth checking, because a drill where")
        print("     every bug is client-visible is not exercising the internal invariants)")
    for r in api_blind:
        print(f"    {r['phase']:<5} {r['name']:<36} caught {r['detected']}/{r['runs']} by "
              f"{r['fired'][:46]}")
    print()
    print(f"  {len(api_blind)} of {caught} caught bugs were invisible from the client API.")
    print()

    return 0 if caught == total and not tally["misbehaving-controls"] else 1


if __name__ == "__main__":
    sys.exit(main())
