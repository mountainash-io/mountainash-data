from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from .config import default_config_files
from .models import HarnessError, Phase
from .process import Redactor
from .runner import LiveDbRunner


def _positive(value: str) -> float:
    number = float(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def _nonnegative(value: str) -> float:
    number = float(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must not be negative")
    return number


def _common_target(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--target", required=True, help="Named live database target.")


def _backend_selection(parser: argparse.ArgumentParser, *, allow_all: bool) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("backend", nargs="?", help="Configured backend name.")
    if allow_all:
        group.add_argument("--all", action="store_true", help="Run every configured backend.")
    else:
        group.add_argument("--all", action="store_true", help=argparse.SUPPRESS)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="live-db", description="Run live database integration tests safely.")
    parser.add_argument("--config", action="append", metavar="PATH", help="Settings file (repeat to set merge order).")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status", help="Show target configuration and suite availability.")
    _common_target(status)

    check = subparsers.add_parser("check", help="Check connectivity without running tests.")
    _common_target(check)
    _backend_selection(check, allow_all=True)
    check.add_argument("--jobs", type=int, metavar="N")
    check.add_argument("--fail-fast", action="store_true")
    check.add_argument("--wait-lock", type=_nonnegative, default=0.0, metavar="SECONDS")

    up = subparsers.add_parser("up", help="Start one Compose service.")
    _common_target(up)
    _backend_selection(up, allow_all=False)
    up.add_argument("--wait-lock", type=_nonnegative, default=0.0, metavar="SECONDS")

    test = subparsers.add_parser("test", help="Run integration tests against a target.")
    _common_target(test)
    _backend_selection(test, allow_all=True)
    test.add_argument("--timeout", type=_positive, metavar="SECONDS")
    test.add_argument("--jobs", type=int, metavar="N")
    test.add_argument("--fail-fast", action="store_true")
    test.add_argument("--wait-lock", type=_nonnegative, default=0.0, metavar="SECONDS")

    down = subparsers.add_parser("down", help="Stop and remove one Compose service.")
    _common_target(down)
    _backend_selection(down, allow_all=False)
    down.add_argument("--wait-lock", type=_nonnegative, default=0.0, metavar="SECONDS")

    run = subparsers.add_parser("run", help="Start, test, and clean up one Compose service.")
    _common_target(run)
    _backend_selection(run, allow_all=True)
    run.add_argument("--timeout", type=_positive, metavar="SECONDS")
    run.add_argument("--jobs", type=int, metavar="N")
    run.add_argument("--fail-fast", action="store_true")
    run.add_argument("--wait-lock", type=_nonnegative, default=0.0, metavar="SECONDS")
    return parser


def _usage_error(parser: argparse.ArgumentParser, detail: str) -> int:
    parser.print_usage()
    print(f"live-db: error: {detail}")
    return 2


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> str | None:
    if args.command in {"check", "test", "run"}:
        if not args.backend and not args.all:
            return "a backend or --all is required"
        if (args.jobs is not None or args.fail_fast) and not args.all:
            return "--jobs and --fail-fast require --all"
        if args.jobs is not None and args.jobs < 1:
            return "--jobs must be greater than zero"
    elif args.command in {"up", "down"}:
        if args.all:
            return f"{args.command} does not support --all"
        if not args.backend:
            return "a backend is required"
    return None


def _config_files(args: argparse.Namespace) -> tuple[Path, ...]:
    if args.config:
        paths = tuple(Path(value).expanduser().resolve() for value in args.config)
        missing = next((path for path in paths if not path.is_file()), None)
        if missing is not None:
            raise HarnessError(None, None, Phase.CONFIGURATION, f"Missing explicit settings file: {missing}", "Provide an existing configuration file.")
        return paths
    root = Path(__file__).resolve().parents[2]
    return default_config_files(root, Path.home())


def main(argv: Sequence[str] | None = None) -> int:
    runner: LiveDbRunner | None = None
    parser = build_parser()
    args = parser.parse_args(argv)
    error = _validate_args(parser, args)
    if error:
        return _usage_error(parser, error)
    try:
        config_files = _config_files(args)
        runner = LiveDbRunner(config_files)
        if args.command == "status":
            return runner.status(args.target)
        if args.command == "check" and args.all:
            results, code = runner.aggregate(args.target, "check", jobs=args.jobs, fail_fast=args.fail_fast, wait_lock=args.wait_lock)
            runner.render(args.target, results)
            return code
        if args.command == "test" and args.all:
            results, code = runner.aggregate(args.target, "test", jobs=args.jobs, fail_fast=args.fail_fast, timeout=args.timeout, wait_lock=args.wait_lock)
            runner.render(args.target, results)
            return code
        if args.command == "run" and args.all:
            results, code = runner.aggregate(args.target, "run", jobs=args.jobs, fail_fast=args.fail_fast, timeout=args.timeout, wait_lock=args.wait_lock)
            runner.render(args.target, results)
            return code
        if args.command == "check":
            result = runner.check_one(args.target, args.backend, wait_lock=args.wait_lock)
        elif args.command == "test":
            result = runner.test_one(args.target, args.backend, timeout=args.timeout, wait_lock=args.wait_lock)
        elif args.command == "up":
            result = runner.up_one(args.target, args.backend, wait_lock=args.wait_lock)
        elif args.command == "down":
            result = runner.down_one(args.target, args.backend, wait_lock=args.wait_lock)
        else:
            result = runner.run_one(args.target, args.backend, timeout=args.timeout, wait_lock=args.wait_lock)
        runner.render(args.target, [result])
        return 0
    except KeyboardInterrupt:
        return 130
    except HarnessError as exc:
        redactor = runner.redactor if runner is not None else Redactor()
        print(redactor(str(exc)))
        if exc.detail.startswith("Command interrupted:"):
            return 130
        return 2 if exc.phase is Phase.CONFIGURATION else 1
