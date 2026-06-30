#!/usr/bin/env python
"""Run various micro-benchmarks."""

from __future__ import annotations

import sys
import pstats

if True:  # ASYNC
    import asyncio

import shlex
import logging
import cProfile
import selectors
import statistics
from time import perf_counter
from typing import Any, Callable
from inspect import isabstract
from argparse import SUPPRESS, Action, ArgumentDefaultsHelpFormatter, ArgumentError
from argparse import ArgumentParser, BooleanOptionalAction, Namespace
from argparse import RawTextHelpFormatter, _SubParsersAction
from functools import cached_property
from contextlib import nullcontext

import psycopg
from psycopg import sql
from psycopg.abc import Query

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
DEFAULT_LOGLEVEL = logging.INFO
LOGGING_OPTIONS = (
    logging.FATAL + 10,
    logging.FATAL,
    logging.ERROR,
    logging.WARN,
    logging.INFO,
    logging.DEBUG,
)


class Printer:
    default = LOGGING_OPTIONS.index(DEFAULT_LOGLEVEL)

    def __init__(self, level=None):
        self.level = level if level is not None else self.default

    def __call__(self, msg):
        if self.level >= self.default:
            print(msg)

    def run(self, func, *args):
        if self.level >= self.default:
            func(*args)

    def title(self, title):
        if self.level >= self.default:
            print(title)
            print("-" * len(title))


pr = cProfile.Profile()


def main():
    args = parse_cmdline()
    out = Printer(LOGGING_OPTIONS.index(args.loglevel))
    if args.loglevel <= logging.FATAL:
        logger.setLevel(args.loglevel)
    else:
        logging.disable()

    if args.subcommand == "compare":
        if args.run:
            logger.fatal("can't yet compare current runs: use '--timing-file'")
            return
        compare(args, out)
        return

    if args.default_selector is not selectors.DefaultSelector:
        selectors.DefaultSelector = args.default_selector  # type: ignore[misc]

    if not args.run:
        out("Nothing to do")
        return

    if True:  # ASYNC
        asyncio.run(setup_and_run(args, out))
    else:
        setup_and_run(args)

    if args.cprofile:
        pr.dump_stats(args.cprofile_file)
        stats = pstats.Stats(pr)
        stats.sort_stats("cumulative")
        out.run(stats.print_stats, 10)


async def setup_and_run(args, out=print):
    async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
        async with conn.cursor() as cur:
            return await run_tests(cur, args, out=out)


def compare(args, out):
    if f := args.compare_file:
        header = (
            "name",
            "min_time",
            "\u0394min_time",
            "%min_time" "avg_time",
            "\u0394avg_time" "%avg_time" "std_dev",
            "\u0394std_dev" "%std_dev",
            "variables",
        )
        if ":" in f:
            f, mode = f.rsplit(":", 1)
        else:
            mode = "x"
        args.compare_file = open(f, mode)
        if "a" not in mode:
            args.compare_file.write("\t".join(header) + "\n")
    timing_sets = get_timing_sets(args.timing_file, args.across)
    if not args.across:
        for timing_set in timing_sets:
            results = compare_set(timing_set)
            output_compare_results(results, out, args.compare_file)
    else:
        results = [Timings() for _ in timing_sets[0]]
        for timing_set in timing_sets:
            r = compare_set(timing_set)
            for i, t in enumerate(r):
                results[i].append(t)
        output_compare_results(
            results[0], out, args.compare_file, output_name=True, is_baseline=True
        )
        for result in results[1:]:
            output_compare_results(
                result,
                out,
                args.compare_file,
                output_name=True,
                is_baseline=False,
            )


def get_timing_sets(timing_files, across=False):
    if across:
        timing_sets: list[Timings] = []
    else:
        timing_sets = [Timings()]
    for i, fname in enumerate(timing_files):
        with open(fname, "r") as f:
            if not across:
                timing_sets[0].extend(timings_from_file(f))
            else:
                timings = timings_from_file(f)
                if (needed := (len(timings) - len(timing_sets))) > 0:
                    timing_sets.extend(Timings() for _ in range(needed))
                for j, timing in enumerate(timings):
                    timing_sets[j].append(timing)
    return timing_sets


def compare_set(timing_set):
    baseline = timing_set[0]
    results = Timings()
    for timing in timing_set:
        results.append(compare_timing(baseline, timing))
    return results


def compare_timing(baseline: Timing, timing: Timing) -> Timing:
    result = Timing([timing[0]])
    result.name = timing.name
    stats = (item for v in timing[1:4] for item in (v, None, None))
    result.extend(stats)  # type: ignore[arg-type]
    result.extend(timing[4:])
    for i in range(1, 4):
        v_base = baseline[i]
        v_new = timing[i]
        assert isinstance(v_new, float)
        assert isinstance(v_base, float)
        j = 2 + 3 * (i - 1)
        diff = result[j] = v_base - v_new
        result[j + 1] = diff / v_base
    return result


def timings_from_file(f):
    timings = Timings()
    constants = None
    name = None
    for line in f:
        if line.startswith("# constants: "):
            _, _, *constants = shlex.split(line)
            timings.constants = constants
        elif line.startswith("# name: "):
            _, _, name = shlex.split(line)
        elif line.startswith("name\tmin_time"):
            continue
        elif not line.startswith("#"):
            timings.append(timing := Timing(line.strip().split("\t")))
            for i in range(1, 4):
                timing[i] = float(timing[i])
            timing[i + 1] = int(timing[i + 1])
    if name is None:
        name = f.name
    for timing in timings:
        timing.name = name
    return timings


def output_compare_results(
    results, out, file=None, output_name=False, is_baseline=None
):
    min_width = max(len(r[0]) + len(r[-1]) + 2 for r in results)
    last_name = None
    for i, result in enumerate(results):
        if is_baseline is None and i == 0:
            is_current_baseline = True
        else:
            is_current_baseline = is_baseline

        if output_name and last_name != result.name:
            if not is_current_baseline:
                out.title(f"{result.name}")
            else:
                out.title(f"Baseline ({result.name})")
            last_name = result.name

        formatted_result = [result[0]]
        formatted_result.extend(
            item
            for i in range(1, len(result) - 2, 3)
            for item in (
                "%.6f" % result[i],
                "%+.6f" % result[i + 1],
                "%+.2f%%" % (result[i + 2] * 100),
            )
        )
        formatted_result.append("%i" % result[-2])
        formatted_result.append(result[-1])
        lead = f"{formatted_result[0]}[{formatted_result[-1]}]:"
        formatted_timings = format_timings(
            (
                formatted_result[1],
                formatted_result[4],
                formatted_result[7],
                formatted_result[10],
            )
        )
        if not is_current_baseline:
            out(
                "{:{min_width}} {:>10} {:>12}\t[{}]".format(
                    lead,
                    formatted_result[3],
                    formatted_result[2],
                    formatted_timings,
                    min_width=min_width,
                )
            )
        else:
            out(
                "{:{min_width}} {}".format(lead, formatted_timings, min_width=min_width)
            )

        if file:
            add_compare_result_to_file(file, formatted_result)
    out("")


def add_compare_result_to_file(file, result):
    file.write("\t".join(result))


class Timing(list[float | int | str]):
    __slots__ = ("name",)
    name: str


class Timings(list[Timing]):
    __slots__ = ("constants",)
    constants: list[str]


async def run_tests(
    cur: psycopg.AsyncCursor, args: Namespace, out: Callable[[str], None] = print
) -> list[tuple[Namespace, list[float]]]:
    timings: list[tuple[Namespace, list[float]]] = []

    variables, constants = get_independent_variables(args.run)

    if f := args.timing_file:
        header = (
            "name",
            "min_time",
            "avg_time",
            "std_dev",
            "num_values",
            "variables" if variables else "parameters",
        )
        if ":" in f:
            f, mode = f.rsplit(":", 1)
        else:
            mode = "x"
        args.timing_file = open(f, mode)
        if "a" not in mode:
            if args.result_name:
                args.timing_file.write(f"# name: {args.result_name}\n")
            args.timing_file.write(f"# constants: {format_bench_options(constants)}\n")
            args.timing_file.write("\t".join(header) + "\n")

    for bench in args.run:
        logger.debug(f"running {format_bench_simple(bench)}")
        test = TestSQL(bench)
        await cur.execute(test.get_table_stmt())
        timing = []
        if bench.executemany:
            insert = executemany_insert
            select = executemany_select
        else:
            insert = execute_insert
            select = execute_select

        old_wait = cur.connection.wait_func
        if wait_func_changed(old_wait, bench.waitfunc):
            old_wait = cur.connection.wait_func
            if isinstance(bench.waitfunc, type):
                cur.connection.wait_func = bench.waitfunc(cur.connection.pgconn.socket)
            else:
                cur.connection.wait_func = bench.waitfunc

        if bench.name in {"copy_out", "select"}:
            await copy_in(bench, test, cur)

        for i in range(bench.repeat):
            if bench.name == "insert":
                t = await insert(bench, test, cur)
                await cur.execute(test.get_truncate_stmt())
            elif bench.name == "insert_returning":
                t = await insert(bench, test, cur, returning="*")
                await cur.execute(test.get_truncate_stmt())
            elif bench.name == "insert_select":
                t = await execute_insert_select(args, test, cur)
                await cur.execute(test.get_truncate_stmt())
            elif bench.name == "copy_in":
                t = await copy_in(bench, test, cur)
                await cur.execute(test.get_truncate_stmt())
            elif bench.name == "select":
                t = await select(bench, test, cur)
            elif bench.name == "copy_out":
                t = await copy_out(bench, test, cur)
            elif bench.name == "connect":
                bench.dsn = args.dsn
                t = await connect(bench, test, cur)
            else:
                raise ValueError(f"Test '{bench.name}' does not exist.")
            timing.append(t)
        await cur.connection.rollback()
        timings.append((bench, timing))
        if wait_func_changed(old_wait, bench.waitfunc):
            if hasattr(cur.connection.wait_func, "close"):
                cur.connection.wait_func.close()
            cur.connection.wait_func = old_wait
        output_timings(out, bench, timing, variables, args.timing_file)

    if args.timing_file:
        args.timing_file.close()

    return timings


def wait_func_changed(old_wait, waitfunc):
    return (
        hasattr(old_wait, "close") and not isinstance(old_wait, waitfunc)
    ) or old_wait is not waitfunc


def get_independent_variables(run):
    constants = Namespace(
        **dict(set.intersection(*(set(vars(b).items()) for b in run)))
    )
    constants.name = ""
    variables = tuple(key for key in vars(run[0]) if key not in constants)
    return variables, constants


def format_timings(formatted_stats):
    return "{} sec [{} +/- {} from {} results]".format(*formatted_stats)


def output_timings(out, bench, timings, variables, file=None):
    if bench.executemany:
        execute_descr = "executemany"
    elif bench.pipeline:
        execute_descr = "pipelined execute"
    else:
        execute_descr = "execute"
    if bench.name in {
        "copy_in",
        "copy_out",
        "insert",
        "insert_returning",
        "select",
        "connect",
    }:
        description = bench.name.replace("_", " ")
    elif bench.name == "insert_select":
        description = "insert then select"
    if bench.name in {"insert", "insert_returning", "insert_select", "select"}:
        description = f"{execute_descr} {description}"

    stats = get_statistics(timings)
    formatted_stats = (
        "%.6f" % stats[0],
        "%.6f" % stats[1],
        "%.6f" % stats[2],
        "%i" % stats[3],
    )
    formatted_variables = format_bench_options(bench, restrict_keys=variables)

    logger.info(format_bench_simple(bench))
    if bench.repeat > 1:
        out(
            "time to {}: {} [{}]".format(
                description,
                format_timings(formatted_stats),
                formatted_variables,
            )
        )
    else:
        out(f"time to {description}: {formatted_stats[0]} sec")

    if file:
        add_timings_to_file(file, bench, formatted_stats, formatted_variables)


def add_timings_to_file(file, bench, stats, variables):
    file.write(
        f"{bench.name.replace("_", "-")}\t" + "\t".join(stats) + "\t" + variables + "\n"
    )


def get_args_from_timing_file(f, safe=True):
    runs = []
    constants = None
    for line in f:
        if line.startswith("# constants: "):
            _, _, *constants = shlex.split(line)
        elif line.startswith("name\tmin_time"):
            continue
        elif not line.startswith("#"):
            name, rest = line.split("\t", 1)
            _, variables = rest.rspit(
                "\t",
            )
            variables = shlex.split(variables)
            runs.append((name, variables))

    if not runs:
        return None
    assert constants is not None

    args: list[str] = []
    if not safe:
        args.extend(f"--{v}" for v in constants)
        args.append("--run")
        args.extend(f"{name}:{",".join(variables)}" for name, variables in runs)
    else:
        args.append("--run")
        args.extend(
            f"{name}:{",".join(variables + constants)}" for name, variables in runs
        )
    return args


def format_bench_simple(bench, joiner=" "):
    return f"{bench.name} [{format_bench_options(bench, joiner)}]"


def _get_bench_value_as_arg(value):
    if hasattr(value, "__name__"):
        return value.__name__.removeprefix("Async").removesuffix("_async")
    return value


def format_bench_options(bench, joiner=" ", restrict_keys=None):
    name = bench.name
    executemany = bench.executemany
    return joiner.join(
        f"{k.replace("_", "-")}={_get_bench_value_as_arg(v)}"
        for k, v in vars(bench).items()
        if k not in {"name", "cprofile"}
        and v is not None
        and (k != "set_types" or name.startswith("copy"))
        and (k != "executemany" or not name.startswith("copy"))
        and (k != "pipeline" or (not name.startswith("copy") and not executemany))
        and (restrict_keys is None or k in restrict_keys)
    )


def get_statistics(timings):
    if len(timings) == 1:
        return (timings[0], timings[0], 0, 1)
    orig_timings = timings
    min_val = min(timings)
    if min_val < 0.2:
        logger.warning(
            "results are not meaningful:"
            + " minimum execution time is less than 0.2 seconds"
        )
    mean_val = statistics.mean(timings)
    stddev = statistics.stdev(timings)

    if len(timings) <= 4:
        return (min_val, mean_val, stddev, len(timings))

    # trim upper outliers
    robust_timings = [t for t in timings if t <= statistics.quantiles(timings)[-1]]
    threshold = statistics.mean(robust_timings) + 3 * statistics.stdev(robust_timings)
    while max(timings) > threshold:
        timings = [t for t in timings if t <= threshold]
        if len(timings) < 4:
            logger.info(
                "Not trimming outliers as it would result in less than 4 timings"
            )
            timings = orig_timings
            break
        mean_val = statistics.mean(timings)
        stddev = statistics.stdev(timings)
        threshold = mean_val + 3 * stddev

    return (min_val, mean_val, stddev, len(timings))


async def connect(args, test, cur):
    connect = psycopg.AsyncConnection.connect
    t0 = perf_counter()
    if args.cprofile:
        pr.enable()
    for _ in range(args.nrecs):
        async with await connect(args.dsn):
            pass
    if args.cprofile:
        pr.disable()
    tf = perf_counter()
    return tf - t0


async def copy_in(args, test, cur):
    async with cur.copy(test.get_copy_stmt(), writer=args.writer) as copy:
        if args.set_types:
            copy.set_types(["text"] * args.nfields)
        records = [test.get_record() for _ in range(args.nrecs)]
        t0 = perf_counter()
        if args.name == "copy_in" and args.cprofile:
            pr.enable()
        for record in records:
            await copy.write_row(record)
        if args.name == "copy_in" and args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


async def copy_out(args, test, cur):
    async with cur.copy(test.get_copy_out_stmt()) as copy:
        if args.set_types:
            copy.set_types(["int4"] + ["text"] * args.nfields)
        t0 = perf_counter()
        if args.cprofile:
            pr.enable()
        while await copy.read_row():
            pass
        if args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


async def executemany_insert(args, test, cur, returning=""):
    insert = test.get_insert_stmt(returning=returning)
    params = [test.get_record() for _ in range(args.nrecs)]
    t0 = perf_counter()
    if args.cprofile:
        pr.enable()
    await cur.executemany(insert, params)
    if args.cprofile:
        pr.disable()
    tf = perf_counter()
    return tf - t0


async def executemany_select(args, test, cur):
    select = test.get_select_stmt()
    params = await test.get_record_ids(cur)
    t0 = perf_counter()
    if args.cprofile:
        pr.enable()
    await cur.executemany(select, params)
    if args.cprofile:
        pr.disable()
    tf = perf_counter()
    return tf - t0


async def execute_insert(args, test, cur, returning=""):
    if args.pipeline:
        context = cur.connection.pipeline()
    else:
        context = nullcontext()
    insert = test.get_insert_stmt(returning=returning)
    params = [test.get_record() for _ in range(args.nrecs)]
    async with context as pipeline:
        t0 = perf_counter()
        if args.cprofile:
            pr.enable()

        for param in params:
            await cur.execute(insert, param)
        if pipeline is not None:
            await pipeline.sync()

        if args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


async def execute_insert_select(args, test, cur):
    if args.pipeline:
        context = cur.connection.pipeline()
    else:
        context = nullcontext()
    insert = test.get_insert_stmt(returning="id")
    select = test.get_select_stmt()
    params = [test.get_record() for _ in range(args.nrecs)]
    with context as pipeline:
        t0 = perf_counter()
        if args.cprofile:
            pr.enable()

        for param in params:
            await cur.execute(insert, param)
            sel_params = await cur.fetchone()
            await cur.execute(select, sel_params)
        if pipeline is not None:
            pipeline.sync()

        if args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


async def execute_select(args, test, cur):
    if args.pipeline:
        context = cur.connection.pipeline()
    else:
        context = nullcontext()
    select = test.get_select_stmt()
    params = await test.get_record_ids(cur)
    async with context as pipeline:
        t0 = perf_counter()
        if args.cprofile:
            pr.enable()

        for param in params:
            await cur.execute(select, param)
        if pipeline is not None:
            await pipeline.sync()

        if args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


class TestSQL:
    def __init__(self, args: Namespace):
        self.args = args

    def get_table_stmt(self) -> Query:
        fields = sql.SQL(", ").join(
            [sql.SQL(f"f{i} text") for i in range(self.args.nfields)]
        )
        stmt = sql.SQL("""\
create temp table testcopy (id serial primary key, {})
""").format(fields)
        return stmt

    def get_copy_stmt(self) -> Query:
        fields = sql.SQL(", ").join(
            [sql.Identifier(f"f{i}") for i in range(self.args.nfields)]
        )
        stmt = sql.SQL("""\
copy testcopy ({}) from stdin{}
""").format(fields, sql.SQL(" WITH (FORMAT BINARY)" if self.args.binary else ""))
        return stmt

    def get_select_stmt(self) -> Query:
        stmt = sql.SQL("""\
SELECT * FROM testcopy WHERE id = {}
""").format(sql.SQL("%b" if self.args.binary else "%t"))
        return stmt

    def get_insert_stmt(self, returning: str = "") -> Query:
        fields = sql.SQL(", ").join(
            [sql.Identifier(f"f{i}") for i in range(self.args.nfields)]
        )
        formatter = sql.SQL("%b" if self.args.binary else "%t")
        stmt = sql.SQL("""\
INSERT INTO testcopy ({}) VALUES ({})
""").format(
            fields,
            sql.SQL(", ").join(formatter for _ in range(self.args.nfields)),
        )
        if returning:
            stmt = sql.SQL(" ").join((stmt, sql.SQL(f"RETURNING {returning}")))
        return stmt

    def get_truncate_stmt(self) -> Query:
        return sql.SQL("TRUNCATE testcopy")

    def get_copy_out_stmt(self) -> Query:
        stmt = sql.SQL("""\
COPY testcopy TO STDOUT{}
""").format(sql.SQL(" WITH (FORMAT BINARY)" if self.args.binary else ""))
        return stmt

    def get_record(self) -> tuple[Any, ...]:
        return tuple("x" * self.args.colsize for _ in range(self.args.nfields))

    async def get_record_ids(self, cur: psycopg.AsyncCursor) -> list[tuple[int]]:
        await cur.execute("SELECT id FROM testcopy")
        return await cur.fetchall()


def compare_parser(subparsers: _SubParsersAction[ArgumentParser]) -> ArgumentParser:
    parser = subparsers.add_parser("compare", aliases=["comp"])
    parser.add_argument(
        "--timing-file",
        help=(
            "compare timings from one or more TIMING_FILE"
            + "\nif `--across` is False, the first timing"
            + " is taken as the baseline."
            + "\nif true, the timings are compared pairwise across "
            + " the files."
        ),
        nargs="+",
        required=True,
    )
    parser.add_argument(
        "--across",
        "-x",
        help="compare across timing files",
        action=FlexibleBooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--compare-file",
        help="compare across timing files",
        action=FlexibleBooleanOptionalAction,
        default=None,
    )
    return parser


def parse_cmdline() -> Namespace:
    parser = ArgumentParser(
        description=__doc__,
        formatter_class=PreserveNewlinesDefaultsFormatter,
        prefix_chars="-@",
        fromfile_prefix_chars="@",
    )
    parser.convert_arg_line_to_args = staticmethod(shlex.split)  # type: ignore
    parser.add_argument("--dsn", default="", help="database connection string")
    valid_actions = {
        "copy-in": "copy in",
        "copy-out": "copy out",
        "insert": "insert",
        "insert-returning": "execute insert returning",
        "insert-select": "insert returning id then select",
        "select": "select",
        "connect": "connect to the database and close the connection NRECS times",
    }
    valid_options = [
        "--repeat",
        "--cprofile",
        "--pipeline",
        "--executemany",
        "--binary",
        "--set-types",
        "--nrecs",
        "--nfields",
        "--colsize",
        "--writer",
        "--waitfunc",
    ]
    parser.add_argument(
        "--run",
        "-r",
        action=ActionsWithOverrides,
        valid_actions=valid_actions,
        valid_options=valid_options,
    )
    parser.add_argument(
        "--run-one",
        "--one",
        "-1",
        action=ActionWithOverrides,
        dest="run",
        valid_actions=valid_actions,
        valid_options=valid_options,
    )
    parser.add_argument(
        "--run-from-timing-file",
        "--from-timing-file",
        help="Repeat the same runs as an existing timing file",
        action=AddArgsFromFileAction,
        file_parser=get_args_from_timing_file,
        nargs="+",
        metavar="TIMING_FILE",
    )
    parser.add_argument(
        "--timing-file",
        "--to-timing-file",
        help=(
            "output timings to FILE"
            + "\nto include ':' in the filename,"
            + " include the MODE (default: x)"
        ),
        metavar="FILE[:MODE]",
    )
    parser.add_argument(
        "--name",
        help="A name for the results (used by the 'compare' subcommand)",
        dest="result_name",
    )
    parser.add_argument("--repeat", type=int, default=1, help="number of repeats")
    parser.add_argument(
        "--pipeline",
        action=FlexibleBooleanOptionalAction,
        default=False,
        help="use pipeline mode for --select and --insert with --no-executemany",
    )
    waitfunc_choices: tuple[str, ...] = (
        "wait_c",
        "wait_selector",
        "wait_select",
        "wait_epoll",
        "wait_poll",
        "wait_kqueue",
        "wait_loop",
    )
    if True:  # ASYNC

        def waitfunc_pred(s):
            return s.startswith("wait_") and s.endswith("_async")

    else:

        def waitfunc_pred(s):
            return s.startswith("wait_") and not s.endswith("_async")

    waitfunc_choices = tuple(s for s in dir(psycopg.waiting) if waitfunc_pred(s))
    parser.add_argument(
        "--waitfunc",
        help="alternative wait function to use",
        action=ModuleAttributeAction,
        module=psycopg.waiting,
        metavar=f"{{{", ".join(waitfunc_choices)}}}",
        default=psycopg.waiting.wait_async,
        transform_async=True,
    )
    parser.add_argument(
        "--default-selector",
        help="Set selectors.DefaultSelector",
        action=ModuleAttributeAction,
        module=selectors,
        default=selectors.DefaultSelector,
        choices=tuple(
            name
            for name, cls in selectors.__dict__.items()
            if name.endswith("Selector")
            and not isabstract(cls)
            and not name[0] == "_"
            and not name == "DefaultSelector"
        ),
    )
    parser.add_argument(
        "--executemany",
        action=FlexibleBooleanOptionalAction,
        default=True,
        help="use executemany instead of execute",
    )
    parser.add_argument(
        "--binary",
        action=FlexibleBooleanOptionalAction,
        default=False,
        help="binary or text output format",
    )
    parser.add_argument(
        "--set-types",
        action=FlexibleBooleanOptionalAction,
        default=False,
        help="call set_types before copy operations",
    )
    parser.add_argument(
        "--nrecs",
        type=int,
        default=1000,
        help="number of records to write",
    )
    parser.add_argument(
        "--nfields",
        type=int,
        default=10,
        help="number of columns to write",
    )
    parser.add_argument(
        "--colsize",
        type=int,
        default=10,
        help="width of each column to write",
    )
    writer_choices: tuple[str, ...] = ("LibpqWriter", "QueuedLibpqWriter")
    if True:  # ASYNC
        writer_choices = tuple(f"Async{s}" for s in writer_choices)
    parser.add_argument(
        "--writer",
        action=ModuleAttributeAction,
        module=psycopg.copy,
        metavar=f"{{{', '.join(writer_choices)}}}",
        help="test alternative writer",
        transform_async=True,
    )
    parser.add_argument(
        "--cprofile",
        action=FlexibleBooleanOptionalAction,
        default=False,
        help="output cProfile information and save profile to CPROFILE_FILE",
    )
    parser.add_argument(
        "--cprofile-file",
        default="output.prof",
        metavar="CPROFILE_FILE",
        help="save cProfile profile to %(metavar)s",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        help="Talk less",
        dest="loglevel",
        action=ChooseConstantAction,
        increment=-1,
        const=LOGGING_OPTIONS,
        default=DEFAULT_LOGLEVEL,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Talk more",
        dest="loglevel",
        action=ChooseConstantAction,
        const=LOGGING_OPTIONS,
        default=DEFAULT_LOGLEVEL,
    )
    subparsers = parser.add_subparsers(
        dest="subcommand",
    )
    compare_parser(subparsers)
    # Added for help text
    # ARGSFILE isn't the right color, but this is close
    parser.add_argument(
        "@ARGSFILE",
        action="store_true",
        help="file providing additional arguments",
        default=None,
    )

    return parser.parse_args()


class ModuleAttributeAction(Action):
    def __init__(self, module, transform_async=False, **kwargs):
        if isinstance(module, str):
            base, *parts = module.split(".")
            module = globals()[base]
            for part in parts:
                module = getattr(module, part)
        self.module = module
        self.transform_async = transform_async
        super().__init__(**kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if not values:
            return
        # little hack to allow passing the same args to the async and sync script
        if self.transform_async:
            if True:  # ASYNC
                if values.lower() == values:
                    if not values.endswith("_async"):
                        values = f"{values}_async"
                else:
                    if not values.startswith("Async"):
                        values = f"Async{values}"
            else:
                values = values.removeprefix("Async").removesuffix("_async")

        try:
            setattr(namespace, self.dest, getattr(self.module, values))
        except AttributeError as ex:
            raise ArgumentError(self, f"unknown {self.dest}: {values!r}") from ex


class PreserveNewlinesDefaultsFormatter(
    ArgumentDefaultsHelpFormatter, RawTextHelpFormatter
):
    def _format_action(self, action):
        self._current_action = action
        return super()._format_action(action)

    def _split_lines(self, text, width):
        if getattr(self._current_action, "preserve_newlines", False):
            return text.splitlines()
        return super()._split_lines(text, width)

    def _get_help_string(self, action):
        if (
            action.default is None
            or action.default == ""
            or not getattr(action, "print_default", True)
        ):
            return action.help
        return super()._get_help_string(action)


class ChooseConstantAction(Action):
    print_default = False

    def __init__(self, option_strings, increment=1, **kwargs):
        kwargs["nargs"] = 0
        self.increment = increment
        super().__init__(option_strings=option_strings, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        value = getattr(namespace, self.dest, None)
        if value is None:
            index = self.const.index(self.default or 0) + self.increment
        else:
            index = self.const.index(value) + self.increment
        index = min(max(0, index), len(self.const) - 1)

        setattr(namespace, self.dest, self.const[index])


class AddArgsFromFileAction(Action):
    def __init__(self, option_strings, file_parser, **kwargs):
        self.file_parser = file_parser
        assert kwargs["nargs"] != 0
        super().__init__(option_strings=option_strings, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        assert values
        if isinstance(values, str):
            values = [values]
        for value in values:
            with open(value, "r") as f:
                args = self.file_parser(f)
            parser.parse_args(args, namespace)


class BaseOverridesAction(Action):
    def __init__(
        self,
        valid_actions,
        valid_options,
        option_strings,
        dest,
        nargs=None,
        help=None,
        metavar=None,
        **kwargs,
    ):
        self.valid_actions = valid_actions
        self.valid_options = valid_options
        if help is None:
            if metavar is not None:
                raise ValueError("'help' must be set if 'metavar' is not None")
            self.preserve_newlines = True
            help = "ACTION is one of the following:"
            if isinstance(valid_actions, dict):
                subtexts = (f"\t{k}: {v}" for k, v in valid_actions.items())
                help = f"{help}\n{'\n'.join(subtexts)}"
            else:
                help = f"{help}\n\t{", ".join(v for v in valid_actions)}"
            help = "\n".join(
                (
                    help,
                    "OPTION is an override for one of the following global options:",
                    f"\t{', '.join(v.lstrip('-') for v in valid_options)}",
                    "See the corresponding global option help text for their use.",
                )
            )

        if nargs is None:
            nargs = "+"

        super().__init__(
            option_strings, dest, nargs, help=help, metavar=metavar, **kwargs
        )

    @cached_property
    def override_parser(self):
        parser = ArgumentParser(exit_on_error=False)

        for action in self.parser._actions:  # type: ignore[attr-defined]
            if action.dest == "help" or action.dest == self.dest:
                continue
            try:
                first_long_option = next(
                    (s for s in action.option_strings if s.startswith("--"))
                )
            except StopIteration:
                continue
            if first_long_option not in self.valid_options:
                continue
            if action.option_strings:
                if not action.type:
                    if isinstance(action, ModuleAttributeAction):
                        parser.add_argument(
                            *action.option_strings,
                            action=type(action),
                            module=action.module,
                            choices=action.choices,
                            default=SUPPRESS,
                            transform_async=action.transform_async,
                        )
                    else:
                        parser.add_argument(
                            *(
                                s
                                for s in action.option_strings
                                if not s.startswith("--no-")
                            ),
                            action=type(action),
                            default=SUPPRESS,
                        )
                else:
                    parser.add_argument(
                        *action.option_strings,
                        type=action.type,
                        choices=action.choices,
                        default=SUPPRESS,
                    )

        return parser

    def create_action(self, name, overrides):
        if overrides is not None:
            lexer = shlex.shlex(overrides, posix=True)
            lexer.whitespace = ","  # Treat comma as a delimiter
            lexer.whitespace_split = True  # Split on the delimiter
            args = [f"--{item}" for item in lexer]
            try:
                action = self.override_parser.parse_args(args)
            except ArgumentError as ex:
                if "unrecognized arguments:" not in ex.message:
                    raise
                raise ArgumentError(
                    self,
                    f"unrecognized options to {name}:"
                    + f" {ex.message.rsplit(": ", 1)[1].replace("-", "")}",
                )
        else:
            action = Namespace()
        action.name = name

        return action


class ActionWithOverrides(BaseOverridesAction):
    def __init__(
        self,
        valid_actions,
        valid_options,
        option_strings,
        dest,
        nargs=None,
        help=None,
        metavar=None,
        **kwargs,
    ):

        help_was_none = help is None

        super().__init__(
            valid_actions,
            valid_options,
            option_strings,
            dest,
            nargs,
            help=help,
            metavar=metavar,
            **kwargs,
        )
        if help_was_none:
            self.help = (
                "Run a single ACTION multiple times with different sets of options."
                + "\n"
                + (self.help or "")
            )

        nargs = self.nargs
        if metavar is None:
            if isinstance(nargs, int) or nargs == "?":
                if nargs == 1 or nargs == "?":
                    self.metavar = "ACTION"
                else:
                    self.metavar = ("ACTION", *("OPTION[=VALUE],...",) * (nargs - 1))
            elif nargs in "*+":
                self.metavar = (
                    "ACTION",
                    "OPTION[=VALUE],... [OPTION[=VALUE],...]",
                )

    def __call__(self, parser, namespace, values, option_string=None):
        previous = getattr(namespace, self.dest, None)
        if values is None:
            return previous
        self.parser = parser

        actions = previous or []

        if isinstance(values, str):
            values = [values]

        name = values[0]
        if name not in self.valid_actions:
            raise ArgumentError(self, f"unrecognized action: {name}")
        name = name.replace("-", "_")

        for overrides in values[1:]:
            actions.append(self.create_action(name, overrides))

        setattr(
            namespace,
            self.dest,
            Actions(
                namespace,
                actions,
                [opt.lstrip("-").replace("-", "_") for opt in self.valid_options],
            ),
        )


class ActionsWithOverrides(BaseOverridesAction):
    def __init__(
        self,
        valid_actions,
        valid_options,
        option_strings,
        dest,
        nargs=None,
        override_delimiter=":",
        help=None,
        metavar=None,
        **kwargs,
    ):
        self.override_delimiter = override_delimiter

        super().__init__(
            valid_actions,
            valid_options,
            option_strings,
            dest,
            nargs,
            help=help,
            metavar=metavar,
            **kwargs,
        )
        if metavar is None:
            self.metavar = f"ACTION[{override_delimiter}OPTION[=VALUE],...]]"

    def __call__(self, parser, namespace, values, option_string=None):
        previous = getattr(namespace, self.dest, None)
        if values is None:
            return previous
        self.parser = parser

        actions = previous or []

        if isinstance(values, str):
            values = [values]

        invalid_names = []
        for value in values:
            try:
                name, overrides = value.split(self.override_delimiter, 1)
            except ValueError:
                name = value
                overrides = None
            if name not in self.valid_actions:
                invalid_names.append(name)
            name = name.replace("-", "_")
            actions.append(self.create_action(name, overrides))

        if invalid_names:
            raise ArgumentError(
                self,
                f"unrecognized actions: {" ".join(invalid_names)}",
            )
        setattr(
            namespace,
            self.dest,
            Actions(
                namespace,
                actions,
                [opt.lstrip("-").replace("-", "_") for opt in self.valid_options],
            ),
        )


class FlexibleBooleanOptionalAction(BooleanOptionalAction):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metavar = "{0, false, 1, true}"
        self.nargs = "?"

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string in self.option_strings:
            if not values:
                value = not option_string.startswith("--no-")
            elif values.lower() in ("0", "false", "off", "no", "f"):
                value = option_string.startswith("--no-")
            elif values.lower() in ("1", "true", "on", "yes", "t"):
                value = not option_string.startswith("--no-")
            else:
                raise ArgumentError(self, f"invalid boolean option: {values}")
            setattr(namespace, self.dest, value)


class Actions(list[Namespace]):
    def __init__(self, namespace, actions_list, valid_options):
        super().__init__(actions_list)
        self.namespace = namespace
        self.valid_options = valid_options

    def __iter__(self):
        for action in super().__iter__():
            yield Namespace(
                **dict(
                    {
                        k: v
                        for k, v in vars(self.namespace).items()
                        if k in self.valid_options
                    },
                    **vars(action),
                )
            )

    def __getitem__(self, index):
        return Namespace(
            **dict(
                {
                    k: v
                    for k, v in vars(self.namespace).items()
                    if k in self.valid_options
                },
                **vars(list.__getitem__(self, index)),
            )
        )


if __name__ == "__main__":
    sys.exit(main())
