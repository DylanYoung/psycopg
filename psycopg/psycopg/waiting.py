"""
Code concerned with waiting in different contexts (blocking, async, etc).

These functions are designed to consume the generators returned by the
`generators` module function and to return their final value.

"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

import os
import sys
import select
import logging
import selectors
from typing import cast
from asyncio import get_running_loop, sleep
from selectors import DefaultSelector

from . import errors as e
from .abc import RV, AsyncWaitFunc, AsyncWaitFuncInstance, PQGen, PQGenConn, WaitFunc
from .abc import WaitFuncInstance
from ._enums import Ready as Ready
from ._enums import Wait as Wait  # re-exported
from ._cmodule import _psycopg

WAIT_R = Wait.R
WAIT_W = Wait.W
WAIT_RW = Wait.RW
READY_NONE = Ready.NONE
READY_R = Ready.R
READY_W = Ready.W
READY_RW = Ready.RW

logger = logging.getLogger(__name__)


if sys.platform != "win32":

    def _check_fd_closed(fileno: int) -> None:
        """
        Raise OperationalError if the connection is lost.
        """
        try:
            os.fstat(fileno)
        except Exception as ex:
            raise e.OperationalError("connection socket closed") from ex

else:

    # On windows we cannot use os.fstat() to check a socket.
    def _check_fd_closed(fileno: int) -> None:
        return


class wait_selector:
    __slots__ = ("selector", "fileno")

    def __init__(self, fileno: int) -> None:
        self.fileno = fileno
        self.open()

    def open(self) -> None:
        self.selector = DefaultSelector()
        try:
            # don't need to register here, but it allows us to use `modify`
            # in `__call__`.
            self.selector.register(self.fileno, WAIT_R)
        except BaseException:
            self.selector.close()
            raise

    def close(self) -> None:
        self.selector.close()

    def __del__(self) -> None:
        self.close()

    def __call__(self, gen: PQGen[RV], fileno: int, interval: float = 0.0) -> RV:
        """
        Wait for a generator using the best strategy available.

        :param gen: a generator performing database operations and yielding
            `Wait` pairs when it would block.
        :param fileno: the file descriptor to wait on.
        :param interval: interval (in seconds) to check for other interrupt, e.g.
            to allow Ctrl-C.
        :return: whatever `!gen` returns on completion.

        Consume `!gen`, scheduling `fileno` for completion when it is reported to
        block. Once ready again send the ready state back to `!gen`.
        """
        if interval is None:
            raise ValueError("indefinite wait not supported anymore")
        try:
            s = old_s = next(gen)
            sel = self.selector
            if not sel.get_map():
                raise e.OperationalError("connection socket closed")
            sel.modify(fileno, s)
            while True:
                if not (rlist := sel.select(timeout=interval)):
                    # Check if it was a timeout or we were disconnected
                    _check_fd_closed(fileno)
                    ready = 0
                else:
                    ready = rlist[0][1]
                s = gen.send(ready)
                if not sel.get_map():
                    raise e.OperationalError("connection socket closed")
                if old_s != s:
                    sel.modify(fileno, s)

        except (OSError, FileNotFoundError) as ex:
            raise e.OperationalError("connection socket closed") from ex
        except StopIteration as ex:
            rv: RV = ex.value
            return rv


def wait_conn(gen: PQGenConn[RV], interval: float = 0.0) -> RV:
    """
    Wait for a connection generator using the best strategy available.

    :param gen: a generator performing database operations and yielding
        (fd, `Wait`) pairs when it would block.
    :param interval: interval (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :return: whatever `!gen` returns on completion.

    Behave like in `wait()`, but take the fileno to wait from the generator
    itself, which might change during processing.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")
    try:
        fileno, s = next(gen)
        with DefaultSelector() as sel:
            sel.register((last_fileno := fileno), (last_s := s))
            while True:
                if not (rlist := sel.select(timeout=interval)):
                    gen.send(READY_NONE)
                    continue

                ready = rlist[0][1]
                fileno, s = gen.send(ready)
                if fileno != last_fileno or last_s != s:
                    sel.unregister(last_fileno)
                    sel.register((last_fileno := fileno), (last_s := s))

    except StopIteration as ex:
        rv: RV = ex.value
        return rv


def _ready_gen(state: Wait) -> PQGen[Ready | int]:
    return (yield state)


def make_wait_async(
    wait: WaitFunc | type[WaitFuncInstance],
) -> AsyncWaitFunc | type[AsyncWaitFuncInstance]:
    outer_wait = wait

    async def wait_async(
        self: AsyncWaitFuncInstance, gen: PQGen[RV], fileno: int, interval: float = 0.0
    ) -> RV:
        if interval is None:
            raise ValueError("indefinite wait not supported anymore")

        ready: Ready | int
        s: Wait

        try:
            s = next(gen)

            # Do this after calling next the first time for performance
            loop = get_running_loop()
            end = loop.time() + interval
            done = False

            def mark_done() -> None:
                nonlocal done
                done = True

            if isinstance(outer_wait, type):
                wait = outer_wait.__call__.__get__(self)
            else:
                wait = outer_wait

            while True:
                ready = wait(_ready_gen(s), fileno, 0.0)
                if not ready:
                    h = loop.call_at(end, mark_done)
                    while True:
                        await sleep(0)
                        ready = wait(_ready_gen(s), fileno, 0.0)
                        if ready:
                            h.cancel()
                            break
                        if done:
                            break

                s = gen.send(ready)
                end = loop.time() + interval
                done = False
        except OSError as ex:
            # Assume the connection was closed
            raise e.OperationalError("connection socket closed") from ex
        except StopIteration as ex:
            rv: RV = ex.value
            return rv

    name = f"wait_{wait.__name__.split('_')[-1]}_async"
    if not isinstance(wait, type):
        wait_async.__name__ = name
        return cast(AsyncWaitFunc, wait_async.__get__(object(), object))

    class wait_async_cls(wait):  # type: ignore[valid-type,misc]
        __call__ = wait_async

    wait_async_cls.__name__ = name

    return wait_async_cls


wait_selector_async = make_wait_async(wait_selector)


async def wait_loop_async(gen: PQGen[RV], fileno: int, interval: float = 0.0) -> RV:
    """
    Coroutine waiting for a generator to complete.

    :param gen: a generator performing database operations and yielding
        `Ready` values when it would block.
    :param fileno: the file descriptor to wait on.
    :param interval: interval (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :return: whatever `!gen` returns on completion.

    Behave like in `wait()`, but exposing an `asyncio` interface.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")

    ready: int
    s: Wait

    try:
        s = next(gen)

        loop = get_running_loop()
        end = loop.time() + interval
        done = False

        def set_done() -> None:
            nonlocal done
            done = True

        def set_ready(state: Ready) -> None:
            nonlocal ready
            ready |= state

        while True:
            reader = s & WAIT_R
            writer = s & WAIT_W
            if not (reader or writer):
                raise e.InternalError(f"bad poll status: {s}")
            ready = 0
            if reader:
                loop.add_reader(fileno, set_ready, READY_R)
            if writer:
                loop.add_writer(fileno, set_ready, READY_W)
            h = loop.call_at(end, set_done)
            try:
                while True:
                    await sleep(0)  # let the loop set ready or done
                    if ready:
                        h.cancel()
                        break
                    if done:
                        break
            finally:
                if reader:
                    loop.remove_reader(fileno)
                if writer:
                    loop.remove_writer(fileno)

            s = gen.send(ready)
            done = False
            end = loop.time() + interval

    except OSError as ex:
        # Assume the connection was closed
        raise e.OperationalError("connection socket closed") from ex
    except StopIteration as ex:
        rv: RV = ex.value
        return rv


async def wait_conn_async(gen: PQGenConn[RV], interval: float = 0.0) -> RV:
    """
    Coroutine waiting for a connection generator to complete.

    :param gen: a generator performing database operations and yielding
        (fd, `Ready`) pairs when it would block.
    :param interval: interval (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :return: whatever `!gen` returns on completion.

    Behave like in `wait()`, but take the fileno to wait from the generator
    itself, which might change during processing.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")

    ready: Ready | int
    s: Wait

    try:
        fileno, s = next(gen)

        loop = get_running_loop()
        end = loop.time() + interval
        done = False

        def set_done() -> None:
            nonlocal done
            done = True

        def set_ready(state: Ready) -> None:
            nonlocal ready
            ready |= state

        while True:
            reader = s & WAIT_R
            writer = s & WAIT_W
            if not (reader or writer):
                raise e.InternalError(f"bad poll status: {s}")

            ready = 0
            if reader:
                loop.add_reader(fileno, set_ready, READY_R)
            if writer:
                loop.add_writer(fileno, set_ready, READY_W)
            h = loop.call_at(end, set_done)
            try:
                while True:
                    await sleep(0)  # let the loop set ready or done
                    if ready:
                        h.cancel()
                        break
                    if done:
                        break
            finally:
                if reader:
                    loop.remove_reader(fileno)
                if writer:
                    loop.remove_writer(fileno)

            fileno, s = gen.send(ready)
            done = False
            end = loop.time() + interval

    except OSError as ex:
        # Assume the connection was closed
        raise e.OperationalError("connection socket closed") from ex
    except StopIteration as ex:
        rv: RV = ex.value
        return rv


# Specialised implementation of wait functions.


def wait_select(gen: PQGen[RV], fileno: int, interval: float = 0.0) -> RV:
    """
    Wait for a generator using select where supported.

    BUG: on Linux, can't select on FD >= 1024. On Windows it's fine.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")
    try:
        s = next(gen)

        empty = ()
        fnlist = (fileno,)
        while True:
            rl, wl, xl = select.select(
                fnlist if s & WAIT_R else empty,
                fnlist if s & WAIT_W else empty,
                fnlist,
                interval,
            )
            if xl:
                _check_fd_closed(fileno)
                # Unlikely: the exception should have been raised above
                raise e.OperationalError("connection socket closed")
            ready = 0
            if rl:
                ready = READY_R
            if wl:
                ready |= READY_W

            s = gen.send(ready)

    except OSError as ex:
        # This happens on macOS but not on Linux (the xl list is set)
        raise e.OperationalError("connection socket closed") from ex
    except StopIteration as ex:
        rv: RV = ex.value
        return rv


wait_select_async = make_wait_async(wait_select)


class wait_epoll:
    __slots__ = ("ep", "transition", "fileno")

    def __init__(self, fileno: int) -> None:
        self.fileno = fileno
        self.open()

        try:
            xx_xx = 0
            er_ew = select.EPOLLIN | select.EPOLLOUT
            er = select.EPOLLIN
            ew = select.EPOLLOUT

            # transition vector: transition[new_state]
            self.transition: list[int] = [xx_xx, er, ew, er_ew]
        except BaseException:
            self.ep.close()
            raise

    def open(self) -> None:
        self.ep = select.epoll()
        try:
            # We don't need to register here but doing so allows us to use
            # `modify` all the time in`__call__`
            self.ep.register(self.fileno)
        except BaseException:
            self.ep.close()
            raise

    def close(self) -> None:
        self.ep.close()

    def __del__(self) -> None:
        self.close()

    def __call__(self, gen: PQGen[RV], fileno: int, interval: float = 0.0) -> RV:
        """
        Wait for a generator using epoll where supported.

        Parameters are like for `wait()`. If it is detected that the best selector
        strategy is `epoll` then this function will be used instead of `wait`.

        See also: https://linux.die.net/man/2/epoll_ctl
        """
        if interval is None:
            raise ValueError("indefinite wait not supported anymore")
        try:
            s = old_s = next(gen)

            if interval < 0:
                interval = 0.0

            if (ep := self.ep).closed:
                raise e.OperationalError("connection socket closed")
            ep.modify(fileno, (transition := self.transition)[s])

            while True:
                ready = 0
                if not (fileevs := ep.poll(interval)):
                    _check_fd_closed(fileno)
                else:
                    ev = fileevs[0][1]
                    if ev & select.EPOLLIN:
                        ready = READY_R
                    if ev & select.EPOLLOUT:
                        ready |= READY_W
                s = gen.send(ready)
                if ep.closed:
                    raise e.OperationalError("connection socket closed")
                if old_s != s:
                    ep.modify(fileno, transition[old_s := s])

        except StopIteration as ex:
            rv: RV = ex.value
            return rv


wait_epoll_async = make_wait_async(wait_epoll)


if hasattr(selectors, "KqueueSelector"):
    from select import KQ_EV_ADD, KQ_EV_DISABLE, KQ_EV_ENABLE, KQ_FILTER_READ
    from select import KQ_FILTER_WRITE, kevent, kqueue

    _kqueue_filters: tuple[int, ...] = (
        -9000,
        KQ_FILTER_READ,  # WAIT_R
        KQ_FILTER_WRITE,  # WAIT_W
    )
else:
    _kqueue_filters = ()


class wait_kqueue:
    __slots__ = ("kq", "transition", "fileno")

    def __init__(self, fileno: int) -> None:
        self.fileno = fileno
        self.open()

        try:
            enable_read = kevent(fileno, KQ_FILTER_READ, flags=KQ_EV_ENABLE)
            enable_write = kevent(fileno, KQ_FILTER_WRITE, flags=KQ_EV_ENABLE)
            disable_read = kevent(fileno, KQ_FILTER_READ, flags=KQ_EV_DISABLE)
            disable_write = kevent(fileno, KQ_FILTER_WRITE, flags=KQ_EV_DISABLE)
            xx_xx: list[kevent] = []
            er_xx = [enable_read]
            xx_ew = [enable_write]
            dr_xx = [disable_read]
            xx_dw = [disable_write]
            er_ew = [enable_read, enable_write]
            dr_dw = [disable_read, disable_write]
            er_dw = [enable_read, disable_write]
            dr_ew = [disable_read, enable_write]

            # transition matrix: transition[old_state][new_state]
            self.transition: list[list[list[kevent]]] = [
                [xx_xx, er_dw, dr_ew, er_ew],
                [dr_xx, xx_xx, dr_ew, xx_ew],
                [xx_dw, er_dw, xx_xx, er_xx],
                [dr_dw, xx_dw, dr_xx, xx_xx],
            ]
        except BaseException:
            self.kq.close()
            raise

    def open(self) -> None:
        fileno = self.fileno
        self.kq = kq = kqueue()
        try:
            kq.control(
                [
                    kevent(fileno, KQ_FILTER_READ, flags=KQ_EV_ADD | KQ_EV_DISABLE),
                    kevent(fileno, KQ_FILTER_WRITE, flags=KQ_EV_ADD | KQ_EV_DISABLE),
                ],
                0,
            )
        except BaseException:
            kq.close()
            raise

    def close(self) -> None:
        self.kq.close()

    def __del__(self) -> None:
        self.close()

    def __call__(
        self,
        gen: PQGen[RV],
        fileno: int,
        interval: float = 0.0,
    ) -> RV:
        """
        Wait for a generator using kqueue where supported.

        Parameters are like for `wait()`. If it is detected that the best selector
        strategy is `kqueue` then this function will be used instead of `wait`.

        See also: https://man.openbsd.org/kqueue.2
        """
        if interval is None:
            raise ValueError("indefinite wait not supported anymore")
        try:
            s = old_s = next(gen)
        except StopIteration as ex:
            rv: RV = ex.value
            return rv

        kq = self.kq
        transition = self.transition
        evs: list[kevent] | None = transition[0][s]
        # TODO: the following two lines are necessary until an open-coded
        # version of wait_kqueue_async
        kq.control(evs, 0)
        evs = None
        try:
            while True:
                ready = 0
                if kq.closed:
                    raise e.OperationalError("connection socket closed")
                if not (events := kq.control(evs, 2, interval)):
                    _check_fd_closed(fileno)
                else:
                    for event in events:
                        if event.filter == KQ_FILTER_READ:
                            ready |= READY_R
                        else:
                            ready |= READY_W
                s = gen.send(ready)
                if s != old_s:
                    evs = transition[old_s][s]
                    old_s = s
                else:
                    evs = None  # don't re-pass the events unless necessary

        except (OSError, FileNotFoundError) as ex:
            # FileNotFound raised when the socket is closed independently
            # OSError is raised on a concurrent close
            raise e.OperationalError("connection socket closed") from ex
        except StopIteration as ex:
            rv = ex.value
            return rv


wait_kqueue_async = make_wait_async(wait_kqueue)


if hasattr(selectors, "PollSelector"):
    _poll_evmasks = {
        WAIT_R: select.POLLIN,
        WAIT_W: select.POLLOUT,
        WAIT_RW: select.POLLIN | select.POLLOUT,
    }
    POLL_BAD = ~(select.POLLIN | select.POLLOUT)
else:
    _poll_evmasks = {}


def wait_poll(gen: PQGen[RV], fileno: int, interval: float = 0.0) -> RV:
    """
    Wait for a generator using poll where supported.

    Parameters are like for `wait()`.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")
    try:
        s = next(gen)

        if interval < 0:
            interval = 0
        else:
            interval = int(interval * 1000.0)

        poll = select.poll()
        evmask = _poll_evmasks[s]
        poll.register(fileno, evmask)
        while True:
            if not (fileevs := poll.poll(interval)):
                gen.send(READY_NONE)
                continue

            ev = fileevs[0][1]

            ready = 0
            if ev & select.POLLIN:
                ready = READY_R
            if ev & select.POLLOUT:
                ready |= READY_W

            if not ready and ev & POLL_BAD:
                _check_fd_closed(fileno)
                # Unlikely: the exception should have been raised above
                raise e.OperationalError("connection socket closed")

            s = gen.send(ready)
            evmask = _poll_evmasks[s]
            poll.modify(fileno, evmask)

    except StopIteration as ex:
        rv: RV = ex.value
        return rv


wait_poll_async = make_wait_async(wait_poll)


def _is_select_patched() -> bool:
    """
    Detect if some greenlet library has patched the select library.

    If this is the case, avoid to use the wait_c function as it doesn't behave
    in a collaborative way.

    Currently supported: gevent.
    """
    # If not imported, don't import it.
    if m := sys.modules.get("gevent.monkey"):
        try:
            if m.is_module_patched("select"):
                return True
        except Exception as ex:
            logger.warning("failed to detect gevent monkey-patching: %s", ex)

    return False


if _psycopg:
    wait_c = _psycopg.wait_c
    wait_c_async = make_wait_async(wait_c)


# Choose the best wait strategy for the platform.
#
# the selectors objects have a generic interface but come with some overhead,
# so we also offer more finely tuned implementations.

wait: WaitFunc | type[WaitFuncInstance]
wait_async: AsyncWaitFunc | type[AsyncWaitFuncInstance]

# Allow the user to choose a specific async function for testing
if "PSYCOPG_ASYNC_WAIT_FUNC" in os.environ:
    fname = os.environ["PSYCOPG_ASYNC_WAIT_FUNC"]
    if (
        not fname.startswith("wait_")
        or not fname.endswith("_async")
        or fname not in globals()
    ):
        raise ImportError(
            "PSYCOPG_ASYNC_WAIT_FUNC should be the name of an available async"
            f" wait function; got {fname!r}"
        )
    wait_async = globals()[fname]

# Allow the user to choose a specific function for testing
if "PSYCOPG_WAIT_FUNC" in os.environ:
    fname = os.environ["PSYCOPG_WAIT_FUNC"]
    if not fname.startswith("wait_") or fname not in globals():
        raise ImportError(
            "PSYCOPG_WAIT_FUNC should be the name of an available wait function;"
            f" got {fname!r}"
        )
    wait = globals()[fname]

# On Windows, for the moment, avoid using wait_c, because it was reported to
# use excessive CPU (see #645).
# TODO: investigate why.
elif _psycopg and sys.platform != "win32" and not _is_select_patched():
    wait = wait_c

elif selectors.DefaultSelector is getattr(selectors, "SelectSelector", None):
    # On Windows, SelectSelector should be the default.
    wait = wait_select

elif selectors.DefaultSelector is getattr(selectors, "KqueueSelector", None):
    # On Mac, KqueueSelector should be the default.
    wait = wait_kqueue

elif selectors.DefaultSelector is getattr(selectors, "EpollSelector", None):
    # On Linux, EpollSelector should be the default.
    wait = wait_epoll

elif selectors.DefaultSelector is getattr(selectors, "PollSelector", None):
    wait = wait_poll

else:
    wait = wait_selector

# default wait_async to the async version of wait
if "wait_async" not in globals():
    wait_async = globals()[wait.__name__ + "_async"]
