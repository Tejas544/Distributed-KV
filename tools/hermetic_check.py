#!/usr/bin/env python3
"""Hermeticity gate for Anvil's deterministic core.

Everything under ``anvil/core/`` must be a pure state machine: no wall clock, no
unseeded entropy, no threads, no syscalls, no environment reads.  Those are not
things you can enforce with code review, because they arrive transitively --
one ``std::chrono::steady_clock::now()`` five headers deep and the determinism
claim is silently false.

So we check the linker's view instead.  A static archive's *strong undefined*
symbols are exactly the set of external facilities it will demand at link time.
If ``clock_gettime`` is in there, the core reads the wall clock, no matter what
the source looks like.

Three tiers, evaluated against the strong-undefined set:

  deny       violation -> exit 1.
  warn       reported, exit unaffected.  For symbols that are usually pulled in
             by libstdc++ internals and are only sometimes our fault.
  allowlist  a specific symbol excused with a written reason and an optional
             expiry date.  Expired entries fail.  Unused entries are reported so
             the file does not rot.

There is also an opt-in ``[allowlist_mode]`` that inverts the whole thing: any
strong-undefined symbol *not* explicitly permitted is a violation.  That is the
real end state -- a denylist can only catch what we thought of -- but it is off
by default until ``anvil_core``'s external surface stops moving.

Weak undefined symbols (nm type ``w``) are reported separately and never fail by
default.  libstdc++ emits weak references to ``pthread_*`` from its single-thread
shims whether or not you use threads; treating those as violations would make
the tool cry wolf on day one and get switched off by week three.

Usage:
    hermetic_check.py build/libanvil_core.a
    hermetic_check.py --json report.json build/libanvil_core.a
    hermetic_check.py --expect-violations test/libanvil_hermetic_negative.a

Exit codes:  0 clean   1 violations   2 tool error
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as _dt
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

if sys.version_info < (3, 11):
    sys.exit("hermetic_check.py requires Python 3.11+ (needs tomllib)")
import tomllib

DEFAULT_CONFIG = Path(__file__).resolve().parent / "hermetic.toml"

# nm output, one symbol per line.  With -A the location prefix is
# "<archive>:<member>:"; undefined symbols carry no address, so the line is
#     libanvil_core.a:buggify.cc.o:                 U clock_gettime@GLIBC_2.17
# Parsing right-to-left avoids fighting the colons inside the location.
_NM_LINE = re.compile(r"^(?P<loc>.*?)\s+(?P<type>[A-Za-z?])\s+(?P<sym>\S+)$")

# glibc versioned symbols: clock_gettime@GLIBC_2.17 / @@GLIBC_2.17
_VERSION_SUFFIX = re.compile(r"@@?[A-Za-z0-9_.]+$")


# --------------------------------------------------------------------------
# model
# --------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Symbol:
    raw: str  # exactly as nm printed it
    name: str  # version suffix stripped
    kind: str  # nm type letter: U (strong undef), w/v (weak undef), ...
    origin: str  # archive member that referenced it

    @property
    def is_strong_undefined(self) -> bool:
        return self.kind == "U"

    @property
    def is_weak_undefined(self) -> bool:
        return self.kind in ("w", "v")


@dataclasses.dataclass
class Rule:
    tier: str  # "deny" | "warn"
    category: str
    why: str
    symbols: frozenset[str]
    patterns: tuple[re.Pattern[str], ...]

    def matches(self, candidates: set[str]) -> bool:
        if self.symbols & candidates:
            return True
        return any(p.search(c) for p in self.patterns for c in candidates)


@dataclasses.dataclass
class Excuse:
    symbol: str
    reason: str
    expires: _dt.date | None
    used: bool = False


@dataclasses.dataclass
class Finding:
    symbol: Symbol
    tier: str
    category: str
    why: str
    demangled: str | None = None

    def render(self) -> str:
        shown = self.demangled or self.symbol.name
        return (
            f"  [{self.category}] {shown}\n"
            f"      referenced by: {self.symbol.origin}\n"
            f"      why forbidden: {self.why}"
        )


# --------------------------------------------------------------------------
# name normalisation
# --------------------------------------------------------------------------


def candidate_names(name: str) -> set[str]:
    """Every spelling a platform might have given the same underlying symbol.

    PE/COFF decorates imports with ``__imp_`` and (on some toolchains) a leading
    underscore; Mach-O prefixes everything with ``_``.  Match against all of
    them so one denylist covers Linux, macOS and Windows.
    """
    out = {name}
    if name.startswith("__imp_"):
        out.add(name[len("__imp_") :])
    for n in list(out):
        # Do not strip from Itanium-mangled names -- _ZN... must stay intact.
        if n.startswith("_") and not n.startswith("_Z") and len(n) > 1:
            out.add(n[1:])
    return out


def strip_version(name: str) -> str:
    return _VERSION_SUFFIX.sub("", name)


# --------------------------------------------------------------------------
# nm
# --------------------------------------------------------------------------


def find_nm(explicit: str | None) -> str:
    for cand in (explicit, os.environ.get("NM"), "llvm-nm", "nm", "gnm"):
        if cand and shutil.which(cand):
            return cand
    sys.exit(
        "error: no usable 'nm' found. Install binutils or LLVM, or pass --nm.\n"
        "       On Windows use the MSYS2/WSL binutils that matches your compiler."
    )


def read_symbols(nm: str, target: Path) -> list[Symbol]:
    if not target.exists():
        sys.exit(f"error: {target} does not exist (build it first)")

    cmd = [nm, "--print-file-name", "--undefined-only", "--no-demangle", str(target)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        # Some nm builds reject --no-demangle; retry without it.
        cmd = [nm, "-A", "-u", str(target)]
        proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(
            f"error: {' '.join(cmd)} failed ({proc.returncode})\n{proc.stderr.strip()}"
        )

    symbols: list[Symbol] = []
    current_member = str(target.name)
    for line in proc.stdout.splitlines():
        line = line.rstrip()
        if not line:
            continue
        # Header-style output ("foo.cc.o:") from nm builds that ignore -A.
        if line.endswith(":") and " " not in line.strip():
            current_member = line[:-1]
            continue
        m = _NM_LINE.match(line)
        if not m:
            continue
        loc = m.group("loc").rstrip(":").strip()
        member = loc.split(":")[-1] if loc else current_member
        raw = m.group("sym")
        symbols.append(
            Symbol(
                raw=raw,
                name=strip_version(raw),
                kind=m.group("type"),
                origin=member or current_member,
            )
        )
    return symbols


def demangle(names: list[str]) -> dict[str, str]:
    """Best-effort Itanium demangling for display only.

    Matching always happens on the mangled name -- length-prefixed identifiers
    mean substrings like ``12system_clock`` appear verbatim in the mangling, so
    the rules work with or without this.  Demangling exists so the error message
    says ``std::thread::_M_start_thread`` instead of ``_ZNSt6thread15...``.
    """
    tool = next((t for t in ("c++filt", "llvm-cxxfilt") if shutil.which(t)), None)
    if not tool or not names:
        return {}
    proc = subprocess.run(
        [tool], input="\n".join(names), capture_output=True, text=True
    )
    if proc.returncode != 0:
        return {}
    out = proc.stdout.splitlines()
    if len(out) != len(names):
        return {}
    return {n: d for n, d in zip(names, out) if d != n}


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------


def load_config(path: Path) -> tuple[list[Rule], dict[str, Excuse], dict]:
    if not path.exists():
        sys.exit(f"error: config not found: {path}")
    with path.open("rb") as fh:
        raw = tomllib.load(fh)

    rules: list[Rule] = []
    for tier in ("deny", "warn"):
        for block in raw.get(tier, []):
            try:
                rules.append(
                    Rule(
                        tier=tier,
                        category=block["category"],
                        why=block["why"],
                        symbols=frozenset(block.get("symbols", [])),
                        patterns=tuple(
                            re.compile(p) for p in block.get("patterns", [])
                        ),
                    )
                )
            except KeyError as exc:
                sys.exit(f"error: [[{tier}]] block missing required key {exc}")
            except re.error as exc:
                sys.exit(f"error: bad regex in [[{tier}]] {block.get('category')}: {exc}")

    excuses: dict[str, Excuse] = {}
    for block in raw.get("allow", []):
        if "symbol" not in block or "reason" not in block:
            sys.exit("error: every [[allow]] entry needs 'symbol' and 'reason'")
        expires = block.get("expires")
        if isinstance(expires, str):
            expires = _dt.date.fromisoformat(expires)
        elif isinstance(expires, _dt.datetime):
            expires = expires.date()
        excuses[block["symbol"]] = Excuse(
            symbol=block["symbol"], reason=block["reason"], expires=expires
        )

    return rules, excuses, raw.get("allowlist_mode", {})


# --------------------------------------------------------------------------
# the check
# --------------------------------------------------------------------------


def evaluate(
    symbols: list[Symbol],
    rules: list[Rule],
    excuses: dict[str, Excuse],
    allowlist_mode: dict,
    today: _dt.date,
) -> tuple[list[Finding], list[Finding], list[Symbol], list[str]]:
    violations: list[Finding] = []
    warnings: list[Finding] = []
    weak: list[Symbol] = []
    notes: list[str] = []

    permitted_pats = (
        tuple(re.compile(p) for p in allowlist_mode.get("permitted", []))
        if allowlist_mode.get("enabled")
        else ()
    )

    for sym in symbols:
        if sym.is_weak_undefined:
            weak.append(sym)
            continue
        if not sym.is_strong_undefined:
            continue

        cands = candidate_names(sym.name)

        excuse = next((excuses[c] for c in cands if c in excuses), None)
        if excuse is not None:
            excuse.used = True
            if excuse.expires is not None and excuse.expires < today:
                violations.append(
                    Finding(
                        symbol=sym,
                        tier="deny",
                        category="expired-exception",
                        why=(
                            f"allowlist entry expired on {excuse.expires}: "
                            f"{excuse.reason}"
                        ),
                    )
                )
            continue

        hit = next((r for r in rules if r.matches(cands)), None)
        if hit is not None:
            (violations if hit.tier == "deny" else warnings).append(
                Finding(symbol=sym, tier=hit.tier, category=hit.category, why=hit.why)
            )
            continue

        if permitted_pats and not any(
            p.search(c) for p in permitted_pats for c in cands
        ):
            violations.append(
                Finding(
                    symbol=sym,
                    tier="deny",
                    category="not-permitted",
                    why=(
                        "allowlist_mode is enabled and this symbol is not in "
                        "[allowlist_mode].permitted"
                    ),
                )
            )

    for exc in excuses.values():
        if not exc.used:
            notes.append(
                f"stale allowlist entry '{exc.symbol}' -- no longer referenced, "
                f"delete it from the config"
            )

    return violations, warnings, weak, notes


def attach_demangled(findings: list[Finding]) -> None:
    mapping = demangle([f.symbol.name for f in findings])
    for f in findings:
        f.demangled = mapping.get(f.symbol.name)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Fail the build if Anvil's core links anything nondeterministic."
    )
    ap.add_argument("targets", nargs="+", type=Path, help="static archives / objects")
    ap.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    ap.add_argument("--nm", default=None, help="override the nm binary")
    ap.add_argument("--json", type=Path, default=None, help="write a JSON report")
    ap.add_argument(
        "--strict-weak",
        action="store_true",
        help="treat weak undefined references as violations too",
    )
    ap.add_argument(
        "--expect-violations",
        action="store_true",
        help="invert the verdict: succeed only if violations are found. "
        "Used by the negative test that proves this gate can fail.",
    )
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    nm = find_nm(args.nm)
    rules, excuses, allowlist_mode = load_config(args.config)
    today = _dt.date.today()

    all_symbols: list[Symbol] = []
    for target in args.targets:
        all_symbols.extend(read_symbols(nm, target))

    if args.strict_weak:
        all_symbols = [
            dataclasses.replace(s, kind="U") if s.is_weak_undefined else s
            for s in all_symbols
        ]

    violations, warnings, weak, notes = evaluate(
        all_symbols, rules, excuses, allowlist_mode, today
    )
    attach_demangled(violations)
    attach_demangled(warnings)

    scanned = " ".join(str(t) for t in args.targets)

    if not args.quiet:
        print(f"hermetic_check: {scanned}")
        print(
            f"  {len(all_symbols)} undefined symbols "
            f"({sum(1 for s in all_symbols if s.is_strong_undefined)} strong, "
            f"{len(weak)} weak) across {len({s.origin for s in all_symbols})} objects"
        )
        for note in notes:
            print(f"  note: {note}")
        if warnings:
            print(f"\n  {len(warnings)} warning(s):")
            for f in warnings:
                print(f.render())
        if violations:
            print(f"\nHERMETICITY VIOLATIONS ({len(violations)}):\n")
            for f in violations:
                print(f.render())
                print()
            print(
                "anvil/core must be a pure state machine. Route this through the\n"
                "Runtime interface (anvil/core/runtime/runtime.h) so the simulator\n"
                "can control it. See docs/SCOPE.md section 4."
            )
        else:
            print("  clean")

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(
                {
                    "targets": [str(t) for t in args.targets],
                    "checked_at": today.isoformat(),
                    "strong_undefined": sum(
                        1 for s in all_symbols if s.is_strong_undefined
                    ),
                    "weak_undefined": len(weak),
                    "violations": [
                        {
                            "symbol": f.symbol.name,
                            "demangled": f.demangled,
                            "category": f.category,
                            "origin": f.symbol.origin,
                            "why": f.why,
                        }
                        for f in violations
                    ],
                    "warnings": [
                        {
                            "symbol": f.symbol.name,
                            "category": f.category,
                            "origin": f.symbol.origin,
                        }
                        for f in warnings
                    ],
                    "notes": notes,
                },
                indent=2,
            )
            + "\n"
        )

    if args.expect_violations:
        if violations:
            if not args.quiet:
                print("\nnegative test passed: the gate detected the seeded violation.")
            return 0
        print(
            "\nNEGATIVE TEST FAILED: expected violations and found none.\n"
            "The hermeticity gate is not actually catching anything -- a green\n"
            "build currently proves nothing. Fix the checker before trusting it.",
            file=sys.stderr,
        )
        return 1

    return 1 if violations else 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        sys.exit(130)
