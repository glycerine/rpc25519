#!/usr/bin/env python3
"""Run a Tube TLC profile using the constant assignments in tube.cfg."""

import argparse
import configparser
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent
KEYWORDS = {"module", "spec", "invariants", "properties", "witnesses", "require_witness", "simulation", "deadlock",
            "inherit_checks", "exclude_invariants"}


def load_profiles():
    profiles = configparser.ConfigParser(interpolation=None)
    profiles.optionxform = str
    profiles.read(ROOT / "checks" / "profiles.ini")
    return profiles


def make_config(profile, witness=None):
    assignments = []
    base_checks = {"invariants": [], "properties": []}
    section = None
    headers = {"CONSTANTS": "constants", "INVARIANT": "invariants",
               "INVARIANTS": "invariants", "PROPERTY": "properties",
               "PROPERTIES": "properties"}
    overrides = {k: (v if v.startswith("<-") else "= " + v)
                 for k, v in profile.items() if k not in KEYWORDS}
    for raw in (ROOT / "tube.cfg").read_text().splitlines():
        line = raw.partition(r"\*")[0].strip()
        if not line:
            continue
        tokens = line.split()
        if tokens[0] in {"SPECIFICATION", "CHECK_DEADLOCK"}:
            section = None
            continue
        if tokens[0] in headers:
            section = headers[tokens.pop(0)]
        if section == "constants" and tokens:
            if len(tokens) < 3 or tokens[1] not in {"=", "<-"}:
                raise ValueError(f"Unexpected assignment in tube.cfg: {line}")
            name, op, *value = tokens
            assignments.append(f"{name} {overrides.pop(name, op+' '+' '.join(value))}")
        elif section in base_checks:
            base_checks[section].extend(tokens)
    if not assignments:
        raise ValueError("Missing CONSTANTS section in tube.cfg")
    # Parameters defined as operators in tube.tla can also be overridden by TLC.
    assignments.extend(f"{name} {setting}" for name, setting in overrides.items())
    lines = ["SPECIFICATION " + profile["spec"], "CHECK_DEADLOCK " + profile.get("deadlock", "FALSE"), "CONSTANTS"]
    lines.extend("    " + a for a in assignments)
    for key, keyword in (("invariants", "INVARIANTS"), ("properties", "PROPERTIES")):
        base_names = base_checks[key] if profile.getboolean("inherit_checks", fallback=False) else []
        if key == "invariants":
            excluded = set(profile.get("exclude_invariants", "").split())
            if excluded.difference(base_names):
                raise ValueError(f"Cannot exclude unknown inherited invariants: {excluded.difference(base_names)}")
            base_names = [name for name in base_names if name not in excluded]
        names = list(dict.fromkeys(base_names + profile.get(key, "").split()))
        if witness and witness[0] == keyword:
            if witness[1] not in names:
                names.append(witness[1])
        if names:
            lines.append(keyword + " " + " ".join(names))
    return "\n".join(lines) + "\n"


def run_profile(name, profile, args, witness=None):
    label = f"{name} ({witness[1]})" if witness else name
    print(f"Checking {label}", flush=True)
    with tempfile.TemporaryDirectory(prefix="tube-tlc-") as temp:
        work = Path(temp)
        for directory in (ROOT, ROOT / "checks"):
            for module in directory.glob("*.tla"):
                (work / module.name).symlink_to(module)
        (work / "model.cfg").write_text(make_config(profile, witness))
        # TLC's simulation trace count is per worker. Preserve the configured
        # sample size and seed when the full suite uses multiple BFS workers.
        workers = 1 if args.all and "simulation" in profile and not witness else args.workers
        cmd = ["java", "-XX:+UseParallelGC", "-Xmx" + args.heap, "-cp", str(args.jar),
               "tlc2.TLC", "-noGenerateSpecTE", "-workers", str(workers), "-config", "model.cfg"]
        if "simulation" in profile and not witness:
            traces, depth = profile["simulation"].split()
            cmd.extend(["-simulate", "num=" + traces, "-depth", depth, "-seed", "25519"])
        cmd.append(profile.get("module", "Update") + ".tla")
        if witness:
            run = subprocess.run(cmd, cwd=work, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, text=True)
            expected = re.search(r"^Error: (?:Invariant|Action property) "
                                 + re.escape(witness[1]) + r"(?: of behavior)? is violated\b",
                                 run.stdout, flags=re.M)
            if run.returncode in {12, 13} and expected:
                print("Reached the milestone excluded by", witness[1])
                print("\n".join(s for s in run.stdout.splitlines()
                                if re.match(r"^State [0-9]+:", s) or "distinct states found" in s))
                return True
            print(run.stdout, end="")
            print(f"TLC did not reach the requested witness {witness[1]}", file=sys.stderr)
            return False
        return subprocess.run(cmd, cwd=work).returncode == 0


def main():
    profiles = load_profiles()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", nargs="*", help="profile names (see --list)")
    parser.add_argument("--all", action="store_true", help="run every profile and its named witnesses")
    parser.add_argument("--list", action="store_true", help="list available profiles")
    parser.add_argument("--witness", action="store_true",
                        help="ask TLC to reach every named milestone for each profile")
    parser.add_argument("--show-config", action="store_true", help="print the generated TLC config")
    jar_dir = Path(os.environ.get("TLA_LIB", ROOT.parent))
    parser.add_argument("--jar", default=os.environ.get("TLA2TOOLS_JAR", jar_dir / "tla2tools.jar"),
                        help="path to tla2tools.jar (defaults to the bundled copy)")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--heap", default="1g", help="maximum Java heap per TLC run (e.g. 2g)")
    args = parser.parse_args()
    if args.list:
        for name in profiles.sections():
            print(f"{name:<22} {profiles[name]['spec']}")
        return 0
    if args.all:
        if args.profile or args.witness:
            parser.error("--all already includes every profile and witness")
        args.profile = profiles.sections()
    if not args.profile:
        parser.error("choose a profile with --list or run --all")
    if not args.show_config:
        if not args.jar or not Path(args.jar).is_file():
            parser.error("set TLA2TOOLS_JAR or pass --jar")
        args.jar = Path(args.jar).resolve()
    for name in args.profile:
        if name not in profiles:
            parser.error(f"unknown profile {name!r}")
        profile = profiles[name]
        witnesses = [("PROPERTIES", n[1:]) if n.startswith("@")
                     else ("INVARIANTS", n) for n in profile.get("witnesses", "").split()]
        if args.witness or profile.getboolean("require_witness", fallback=False):
            if not witnesses:
                parser.error(f"{name} has no separate reachability probe")
            probes = witnesses
        else:
            probes = [None] + (witnesses if args.all else [])
        for witness in probes:
            if args.show_config:
                print(make_config(profile, witness), end="")
            elif not run_profile(name, profile, args, witness):
                return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
