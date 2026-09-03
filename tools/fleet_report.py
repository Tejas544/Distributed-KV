#!/usr/bin/env python3
"""Aggregate a seed fleet's JSONL into a report, and file candidate ledger rows.

    tools/fleet_report.py /tmp/fleet/*.jsonl

The fleet records; this decides what is worth a human's attention. Three jobs,
and the third is the one that makes a large fleet usable at all:

  1. Report the honest scale: simulated node-hours, not wall clock, plus the
     speedup that bought them.
  2. Deduplicate failures by (workload, invariant, minimised fault signature).
     The first thing a fleet produces at volume is four hundred copies of one
     bug; a report that does not collapse them is a report nobody reads.
  3. Separate *understood* failures from *new* ones. A signature that is already
     classified -- with the argument written down here, next to it -- is noise
     the fleet will keep producing forever, and burying a new finding under it
     is the failure mode that makes people stop reading fleet output.

Only the new ones get candidate ledger rows.
"""

import glob
import json
import sys
from collections import defaultdict

# ---------------------------------------------------------------------------
# Failures that are already understood, and why.
#
# The rule for adding to this table: an entry needs an argument that would
# convince somebody who wanted to believe it was a bug. "We see this a lot" is
# not one. Each entry is keyed by (workload, invariant) and matched against the
# minimised fault signature, so a *different* signature for the same invariant
# is still reported as new -- which is the point, because that is exactly how a
# real bug hides behind a known one.
# ---------------------------------------------------------------------------
#
# One caveat that belongs at the top rather than buried: **the fleet's
# classification is deliberately narrower than each suite's own.** raft_faults.cc
# knows, for instance, that a lease overlap on a seed whose clock model was told
# to exceed its own declared bound is the environment breaking its promise rather
# than the protocol breaking its; the fleet does not, because it runs the
# workloads and not the suites. So the "new" list below is an *over-approximation*
# -- a triage queue, not a list of bugs. That is the right direction for a fleet
# to be wrong in, and the entries here are the ones that have been reconciled.
KNOWN = {
    ("raft_kv", "INV-RAFT-13"): [
        (
            "clock.",
            "A lease is an optimisation licensed by a clock bound: it is safe "
            "only while the real error is smaller than the bound the "
            "configuration declared. The fault profile can deliberately exceed "
            "its own declared bound, and on a minimised set containing a clock "
            "fault that is exactly what happened -- the environment broke the "
            "promise the lease rests on. test/raft_faults.cc classifies these "
            "the same way and counts them rather than failing on them. A "
            "signature with NO clock fault in it is not this case and is a real "
            "finding.",
        ),
    ],
    ("counter", "INV-CTR-01"): [
        (
            "disk.bit_rot",
            "A single-replica write-ahead log with no erasure coding cannot "
            "survive a flipped bit in a record it already wrote -- there is "
            "nowhere to recover it from. What it must do is *notice*, and the "
            "checksum does. Detected loss and silent corruption are different "
            "outcomes and only one of them is a bug; test/sim_faults.cc holds "
            "bit-rot seeds to that standard and so does this. A signature "
            "without disk.bit_rot in it is NOT this case.",
        ),
    ],
    ("counter", "INV-CTR-02"): [
        ("disk.bit_rot", "Same as INV-CTR-01: media damage on a single replica."),
    ],
    ("counter", "INV-CTR-03"): [
        ("disk.bit_rot", "Same as INV-CTR-01: media damage on a single replica."),
    ],
    ("counter", "INV-CTR-durability"): [
        ("disk.bit_rot", "Same as INV-CTR-01: media damage on a single replica."),
    ],
}


def classify(workload, invariant, signature):
    """Returns the argument if this failure is already understood, else None."""
    for required, argument in KNOWN.get((workload, invariant), []):
        if required in signature:
            return argument
    return None


def main(paths):
    files = []
    for p in paths:
        files.extend(glob.glob(p))
    if not files:
        print("no fleet output found; give me the JSONL files", file=sys.stderr)
        return 2

    runs = 0
    node_nanos = 0
    by_workload = defaultdict(lambda: {"runs": 0, "failed": 0})
    classes = defaultdict(lambda: {"seeds": [], "detail": "", "one_minimal": 0})

    for path in files:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                r = json.loads(line)
                runs += 1
                node_nanos += r["node_nanos"]
                w = by_workload[r["workload"]]
                w["runs"] += 1
                if not r["violated"]:
                    continue
                w["failed"] += 1
                key = (r["workload"], r["invariant"], r["signature"])
                c = classes[key]
                c["seeds"].append(r["seed"])
                c["detail"] = c["detail"] or r["detail"]
                c["one_minimal"] += 1 if r["one_minimal"] else 0

    node_hours = node_nanos / 3.6e12

    print("=" * 78)
    print("SEED FLEET REPORT")
    print("=" * 78)
    print()
    print(f"  runs ............................ {runs:,}")
    print(f"  simulated node-hours ............ {node_hours:,.1f}")
    print(f"  shards .......................... {len(files)}")
    print()
    print("  per workload:")
    for name in sorted(by_workload):
        w = by_workload[name]
        rate = 100.0 * w["failed"] / w["runs"] if w["runs"] else 0.0
        print(f"    {name:<12} {w['runs']:>7,} runs   {w['failed']:>6,} with a violation "
              f"({rate:5.1f}%)")

        # The premise check. A workload that fires on essentially every seed is
        # a broken harness, not a discovery, and filing two thousand rows for it
        # would bury everything else.
        if w["runs"] >= 20 and rate > 90.0:
            print(f"      *** {name} fails on {rate:.0f}% of seeds. That is a harness "
                  f"problem, not {w['failed']} findings. Fix it before reading further.")

    print()
    print("-" * 78)
    print("DISTINCT FAILURE CLASSES  (workload, invariant, minimised fault signature)")
    print("-" * 78)

    if not classes:
        print("\n  none.\n")
        return 0

    understood, new = [], []
    for key, c in classes.items():
        (understood if classify(*key) else new).append((key, c))

    def show(rows, header):
        if not rows:
            return
        print()
        print(header)
        for (workload, invariant, signature), c in sorted(
            rows, key=lambda kv: -len(kv[1]["seeds"])
        ):
            seeds = sorted(c["seeds"])
            shown = ", ".join(str(s) for s in seeds[:8])
            more = f" (+{len(seeds) - 8} more)" if len(seeds) > 8 else ""
            print()
            print(f"  {workload} / {invariant}")
            print(f"    minimised to ... {signature}")
            print(f"    seeds .......... {len(seeds)}: {shown}{more}")
            print(f"    1-minimal ...... {c['one_minimal']}/{len(seeds)} verified")
            if c["detail"]:
                print(f"    first detail ... {c['detail'][:150]}")
            argument = classify(workload, invariant, signature)
            if argument:
                print(f"    classified ..... {argument}")

    show(understood, "ALREADY UNDERSTOOD -- no row, see the argument beside each:")
    show(new, "NEW -- candidate ledger rows:")

    if new:
        print()
        print("-" * 78)
        print("CANDIDATE BUGS.md ROWS")
        print("-" * 78)
        print()
        print("  Paste, then do the work the fleet cannot: decide whether each is the")
        print("  engine or the harness. Roughly a third of this project's findings have")
        print("  been the harness, and three of those manufactured findings out of")
        print("  nothing, so that question comes first and not last.")
        for (workload, invariant, signature), c in sorted(
            new, key=lambda kv: -len(kv[1]["seeds"])
        ):
            seed = sorted(c["seeds"])[0]
            print()
            print("```yaml")
            print("id:              ANV-XXXX")
            print(f"title:           {invariant} fires on {workload} "
                  f"with {signature}")
            print("status:          open")
            print("severity:        S?           # decide: client-visible or internal")
            print("class:           safety | liveness | test infrastructure")
            print(f"invariant:       {invariant}")
            print(f"layer:           {workload}")
            print("found_by:        dst-random   # the seed fleet")
            print("api_visible:     ?            # be strict; overstating this is fatal")
            print(f"seed:            {seed}")
            print(f"config:          SimConfig::from_seed({seed}), workload {workload}")
            print(f"runs_to_first_hit: {len(c['seeds'])} in the sweep")
            print(f"faults_minimised: [{signature}]")
            print(f"root_cause: |\n  TODO. First: engine or harness?")
            print(f"regression:      test/corpus/ANV-XXXX.seed")
            print("```")

    print()
    print(f"{len(classes)} distinct classes: {len(understood)} understood, {len(new)} new")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["/tmp/fleet/*.jsonl"]))
