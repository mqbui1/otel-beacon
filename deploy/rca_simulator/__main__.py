"""
Stripped-down CLI entry point — only the 'run' subcommand is needed
when invoked from the otel-beacon scenario controller.

Usage:
  python3 -m rca_simulator run --scenario db_slowdown --topology-file /app/petclinic-topology.yaml
"""

import argparse
import logging
import os
import signal

from .emitter import setup
from .runner import ScenarioRunner
from .scenarios import SCENARIOS
from .topology_loader import load_topology


def _configure_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    if not verbose:
        logging.getLogger("opentelemetry").setLevel(logging.WARNING)


def cmd_list(args):
    print("\n--- Available scenarios ---\n")
    for name, cls in SCENARIOS.items():
        s = cls()
        print(f"  {name:<22} {s.description}")
        print(f"  {'':22} Affected: {s.affected_service} > {s.affected_operation}\n")


def cmd_run(args):
    _configure_logging(args.verbose)
    env = args.env or os.getenv("DEPLOYMENT_ENV", "rca-demo")

    topology = None
    if args.topology_file:
        topology = load_topology(args.topology_file)
        env = topology.environment
        print(f"Loaded topology: {args.topology_file}  (env={env})")

    setup(env=env)

    runner = ScenarioRunner(
        scenario_name=args.scenario,
        rps=args.rps,
        warmup_s=args.warmup,
        anomaly_s=args.anomaly,
        cooldown_s=args.cooldown,
        anomaly_pct=args.anomaly_pct,
        topology=topology,
    )

    def handle_sigint(sig, frame):
        print("\nInterrupted. Flushing...")
        runner.stop()

    signal.signal(signal.SIGINT, handle_sigint)
    runner.run()


def main():
    parser = argparse.ArgumentParser(description="RCA Simulator scenario runner")
    parser.add_argument("--verbose", "-v", action="store_true")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("list", help="List available scenarios")

    run_p = sub.add_parser("run", help="Run a scenario")
    run_p.add_argument("--scenario", "-s", required=True)
    run_p.add_argument("--topology-file", "-t", default=None)
    run_p.add_argument("--rps", type=float, default=3.0)
    run_p.add_argument("--warmup", type=int, default=20)
    run_p.add_argument("--anomaly", type=int, default=90)
    run_p.add_argument("--cooldown", type=int, default=20)
    run_p.add_argument("--anomaly-pct", type=float, default=0.8)
    run_p.add_argument("--env", default=None)
    run_p.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    if args.command == "list" or args.command is None:
        cmd_list(args)
    elif args.command == "run":
        cmd_run(args)


if __name__ == "__main__":
    main()
