"""Command line entry point.

    python -m levy_curate scaffold  configs/geotiffs.yaml
    python -m levy_curate validate  configs/geotiffs.yaml
    python -m levy_curate plan      configs/geotiffs.yaml
    python -m levy_curate deposit   configs/geotiffs.yaml
    python -m levy_curate reconcile configs/geotiffs.yaml
    python -m levy_curate harvest   configs/geotiffs.yaml
    python -m levy_curate status    configs/geotiffs.yaml
"""

from __future__ import annotations

import argparse
import sys

from . import inventory, scaffold
from .config import BatchConfig
from .manifest import Manifest


def _cfg(args) -> BatchConfig:
    cfg = BatchConfig.from_yaml(args.config)
    if getattr(args, "target", None):
        cfg.target = args.target
    return cfg


def cmd_scaffold(args) -> int:
    cfg = _cfg(args)
    result = scaffold.build(cfg, partial=args.partial, write=not args.dry_run)
    print(result.report())
    return 0 if result.complete else 1


def cmd_validate(args) -> int:
    cfg = _cfg(args)
    df = inventory.load(cfg)
    rep = inventory.validate(df, cfg)
    print(rep.report())
    if len(rep.missing_on_disk):
        out = cfg.root / f"{cfg.name}_missing_files.csv"
        rep.missing_on_disk.to_csv(out, index=False)
        print(f"  wrote {out}")
    if len(rep.missing_in_table):
        out = cfg.root / f"{cfg.name}_missing_metadata.csv"
        rep.missing_in_table.to_csv(out, index=False)
        print(f"  wrote {out}")
    return 0 if rep.ok else 1


def cmd_reconcile(args) -> int:
    return cmd_validate(args)


def cmd_plan(args) -> int:
    from . import deposit as dep

    cfg = _cfg(args)
    df = inventory.load(cfg)
    rep = inventory.validate(df, cfg)
    if not rep.ok:
        print(rep.report())
        print("\nrefusing to plan an invalid inventory")
        return 1
    print(dep.plan(df, cfg))
    return 0


def cmd_deposit(args) -> int:
    from . import deposit as dep

    cfg = _cfg(args)
    df = inventory.load(cfg)
    rep = inventory.validate(df, cfg)
    if not rep.ok:
        print(rep.report())
        print("\nrefusing to deposit an invalid inventory")
        return 1

    creds = cfg.credentials
    if creds.target == "harvard" and not args.yes:
        print(dep.plan(df, cfg))
        if input("\nDeposit to PRODUCTION. Type the batch name to confirm: ") != cfg.name:
            print("aborted")
            return 1

    man = dep.deposit(df, cfg)
    print()
    print(man.report())
    return 0


def cmd_harvest(args) -> int:
    from . import harvest as hv

    cfg = _cfg(args)
    df = hv.harvest_batch(cfg, version=args.version)
    print(f"harvested {len(df)} file DOI(s)")
    return 0


def cmd_status(args) -> int:
    cfg = _cfg(args)
    print(cfg.summary())
    print()
    print(Manifest(cfg.manifest_path(), cfg.name).report())
    return 0


COMMANDS = {
    "scaffold": cmd_scaffold,
    "validate": cmd_validate,
    "reconcile": cmd_reconcile,
    "plan": cmd_plan,
    "deposit": cmd_deposit,
    "harvest": cmd_harvest,
    "status": cmd_status,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="levy_curate", description=__doc__)
    parser.add_argument("command", choices=sorted(COMMANDS))
    parser.add_argument("config", help="path to a batch config YAML")
    parser.add_argument("--target", choices=("demo", "harvard"), help="override DATAVERSE_TARGET")
    parser.add_argument("--partial", help="scaffold: path to the partial metadata CSV")
    parser.add_argument("--dry-run", action="store_true", help="scaffold: do not write output")
    parser.add_argument("--version", default=":draft", help="harvest: dataset version")
    parser.add_argument("--yes", action="store_true", help="deposit: skip production confirmation")
    args = parser.parse_args(argv)

    try:
        return COMMANDS[args.command](args)
    except (FileNotFoundError, KeyError, ValueError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
