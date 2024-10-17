# This file is part of Maker Keeper Framework.
#
# Copyright (C) 2017-2018 reverendus
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import logging
import signal
import threading
import time
import traceback
import json
import asyncio

import pytz
from pymaker.sign import eth_sign
from web3 import Web3, AsyncWeb3, WebsocketProviderV2
from web3.exceptions import BlockNotFound, BlockNumberOutofRange
from web3.types import HexBytes

from websockets import connect

from pymaker import register_filter_thread, any_filter_thread_present, stop_all_filter_threads, all_filter_threads_alive
from pymaker.util import AsyncCallback


def trigger_event(event: threading.Event):
    assert(isinstance(event, threading.Event))

    event.set()


class Lifecycle:
    """Main keeper lifecycle controller.

    This is a utility class helping to build a proper keeper lifecycle. Lifecycle
    consists of startup phase, subscribing to Web3 events and/or timers, and
    a shutdown phase at the end.

    One could as well initialize the keeper and start listening for events themselves
    i.e. without using `Lifecycle`, just that this class takes care of some quirks.
    For example the listener threads of web3.py tend to die at times, which causes
    the client to stop receiving events without even knowing something might be wrong.
    `Lifecycle` does some tricks to monitor for it, and shutdowns the keeper the
    moment it detects something may be wrong with the listener threads.

    Other quirk is the new block filter callback taking more time to execute that
    the time between subsequent blocks. If you do not handle it explicitly,
    the event queue will pile up and the keeper won't work as expected.
    `Lifecycle` used :py:class:`pymaker.util.AsyncCallback` to handle it properly.

    It also handles:
    - waiting for the node to have at least one peer and sync before starting the keeper,
    - checking if the keeper account (`web3.eth.default_account`) is unlocked.

    Also, once the lifecycle is initialized, keeper starts listening for SIGINT/SIGTERM
    signals and starts a graceful shutdown if it receives any of them.

    The typical usage pattern is as follows:

        with Web3Lifecycle(self.web3) as lifecycle:
            lifecycle.on_startup(self.some_startup_function)
            lifecycle.on_block(self.do_something)
            lifecycle.every(15, self.do_something_else)
            lifecycle.on_shutdown(self.some_shutdown_function)

    once called like that, `Lifecycle` will enter an infinite loop.

    Attributes:
        web3: Instance of the `Web3` class from `web3.py`. Optional.
    """
    logger = logging.getLogger()

    def __init__(self, web3: Web3 = None):
        self.web3 = web3

        self.do_wait_for_sync = True
        self.delay = 0
        self.wait_for_functions = []
        self.startup_function = None
        self.shutdown_function = None
        self.block_function = None
        self.every_timers = []
        self.event_timers = []

        """ web3 client specific tuning options """
        self.skip_peer_check = False
        self.skip_syncing_check = False
        """ use latest block on new block event """
        self.new_block_callback_use_latest = False
        """ Use eth_subscribe for new blocks instead of eth_newBlockFilter. (e.g. Erigon does not support eth_filter) """
        self.subscribe_new_heads = False

        self.terminated_internally = False
        self.terminated_externally = False
        self.fatal_termination = False
        self._at_least_one_every = False
        self._last_block_time = None
        self._on_block_callback = None
        self._max_block_number = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Initialization phase
        if self.web3:
            self.logger.info(f"Lifecycle: Keeper connected to {self.web3.provider}")
            if self.web3.eth.default_account and self.web3.eth.default_account != "0x0000000000000000000000000000000000000000":
                self.logger.info(f"Lifecycle: Keeper operating as {self.web3.eth.default_account}")
                self._check_account_unlocked()
            else:
                self.logger.info(f"Lifecycle: Keeper not operating as any particular account")
                # web3 calls do not work correctly if defaultAccount is empty
                self.web3.eth.default_account = "0x0000000000000000000000000000000000000000"
        else:
            self.logger.info(f"Lifecycle: Keeper initializing")

        # Wait for sync and peers
        if self.web3 and self.do_wait_for_sync:
            self._wait_for_init()

        # Initial delay
        if self.delay > 0:
            self.logger.info(f"Lifecycle: Waiting for {self.delay} seconds of initial delay...")
            time.sleep(self.delay)

        # Initial checks
        if len(self.wait_for_functions) > 0:
            self.logger.info("Lifecycle: Waiting for initial checks to pass...")

            for index, (wait_for_function, max_wait) in enumerate(self.wait_for_functions, start=1):
                start_time = time.time()
                while True:
                    try:
                        result = wait_for_function()
                    except Exception as e:
                        self.logger.exception(f"Lifecycle: Initial check #{index} failed with an exception: '{e}'")
                        result = False

                    if result:
                        break

                    if time.time() - start_time >= max_wait:
                        self.logger.warning(f"Lifecycle: Initial check #{index} took more than {max_wait} seconds to pass, skipping")
                        break

                    time.sleep(0.1)

        # Startup phase
        if self.startup_function:
            self.logger.info("Lifecycle: Executing keeper startup logic")
            self.startup_function()

        # Bind `on_block`, bind `every`
        # Enter the main loop
        self._start_watching_blocks()
        self._start_every_timers()
        self._main_loop()

        # Enter shutdown process
        self.logger.info("Lifecycle: Shutting down the keeper")

        # Disable all filters
        if any_filter_thread_present():
            self.logger.info("Lifecycle: Waiting for all threads to terminate...")
            stop_all_filter_threads()

        # If the `on_block` callback is still running, wait for it to terminate
        if self._on_block_callback is not None:
            self.logger.info("Lifecycle: Waiting for outstanding callback to terminate...")
            self._on_block_callback.wait()

        # If any every (timer) callback is still running, wait for it to terminate
        if len(self.every_timers) > 0:
            self.logger.info("Lifecycle: Waiting for outstanding timers to terminate...")
            for timer in self.every_timers:
                timer[1].wait()

        # If any event callback is still running, wait for it to terminate
        if len(self.event_timers) > 0:
            self.logger.info("Lifecycle: Waiting for outstanding events to terminate...")
            for timer in self.event_timers:
                timer[2].wait()

        # Shutdown phase
        if self.shutdown_function:
            self.logger.info("Lifecycle: Executing keeper shutdown logic...")
            self.shutdown_function()
            self.logger.info("Lifecycle: Shutdown logic finished")
        self.logger.info("Lifecycle: Keeper terminated")
        # exit(10 if self.fatal_termination else 0)

    def _wait_for_init(self):
        # In unit-tests waiting for the node to sync does not work correctly.
        # So we skip it.
        # if 'TestRPC' in self.web3.client_version:
        #     return

        # wait for the client to have at least one peer
        if not self.skip_peer_check:
            try:
                if self.web3.net.peer_count == 0:
                    self.logger.info(f"Lifecycle: Waiting for the node to have at least one peer...")
                    while self.web3.net.peer_count == 0:
                        time.sleep(0.25)
                        if self.terminated_internally or self.terminated_externally:
                            self.logger.info(f"Lifecycle: terminated...")
                            break
            except Exception as err:
                if 'unauthorized method' in str(err).lower():
                    pass
                else:
                    raise err

        # wait for the client to sync completely,
        # as we do not want to apply keeper logic to stale blocks
        if not self.skip_syncing_check:
            try:
                if self.web3.eth.syncing:
                    self.logger.info(f"Lifecycle: Waiting for the node to sync...")
                    while self.web3.eth.syncing:
                        time.sleep(0.25)
                        if self.terminated_internally or self.terminated_externally:
                            self.logger.info(f"Lifecycle: terminated...")
                            break
            except Exception as err:
                if 'unauthorized method' in str(err).lower():
                    pass
                else:
                    raise err

    def _check_account_unlocked(self):
        try:
            eth_sign(bytes("pymaker testing if account is unlocked", "utf-8"), self.web3)
        except:
            self.logger.exception(f"Lifecycle: Account {self.web3.eth.default_account} is not unlocked and no private key supplied for it")
            self.logger.fatal(f"Lifecycle: Unlocking the account or providing the private key is necessary for the keeper to operate")
            exit(-1)

    def wait_for_sync(self, wait_for_sync: bool):
        assert(isinstance(wait_for_sync, bool))

        self.do_wait_for_sync = wait_for_sync

    def initial_delay(self, initial_delay: int):
        """Make the keeper wait for specified amount of time before startup.

        The primary use case is to allow background threads to have a chance to pull necessary
        information like prices, gas prices etc. At the same time we may not want to wait indefinitely
        for that information to become available as the price source may be down etc.

        Args:
            initial_delay: Initial delay on keeper startup (in seconds).
        """
        assert(isinstance(initial_delay, int))

        self.delay = initial_delay

    def wait_for(self, initial_check, max_wait: int):
        """Make the keeper wait for the function to turn true before startup.

        The primary use case is to allow background threads to have a chance to pull necessary
        information like prices, gas prices etc. At the same time we may not want to wait indefinitely
        for that information to become available as the price source may be down etc.

        Args:
            initial_check: Function which will be evaluated and its result compared to True.
            max_wait: Maximum waiting time (in seconds).
        """
        assert(callable(initial_check))
        assert(isinstance(max_wait, int))

        self.wait_for_functions.append((initial_check, max_wait))

    def on_startup(self, callback):
        """Register the specified callback to be run on keeper startup.

        Args:
            callback: Function to be called on keeper startup.
        """
        assert(callable(callback))

        assert(self.startup_function is None)
        self.startup_function = callback

    def on_shutdown(self, callback):
        """Register the specified callback to be run on keeper shutdown.

        Args:
            callback: Function to be called on keeper shutdown.
        """
        assert(callable(callback))

        assert(self.shutdown_function is None)
        self.shutdown_function = callback

    def terminate(self, message=None):
        if message is not None:
            if 'Lifecycle' not in message:
                message = f"Lifecycle: {message}"
            self.logger.warning(message)

        self.terminated_internally = True

    def on_block(self, callback):
        """Register the specified callback to be run for each new block received by the node.

        Args:
            callback: Function to be called for each new blocks.
        """
        assert(callable(callback))

        assert(self.web3 is not None)
        assert(self.block_function is None)
        self.block_function = callback

    def on_event(self, event: threading.Event, min_frequency_in_seconds: int, callback):
        """
        Register the specified callback to be called every time event is triggered,
        but at least once every `min_frequency_in_seconds`.

        Args:
            event: Event which should be monitored.
            min_frequency_in_seconds: Minimum execution frequency (in seconds).
            callback: Function to be called by the timer.
        """
        assert(isinstance(event, threading.Event))
        assert(isinstance(min_frequency_in_seconds, int))
        assert(callable(callback))

        self.event_timers.append((event, min_frequency_in_seconds, AsyncCallback(callback)))

    def every(self, frequency_in_seconds: int, callback):
        """Register the specified callback to be called by a timer.

        Args:
            frequency_in_seconds: Execution frequency (in seconds).
            callback: Function to be called by the timer.
        """
        self.every_timers.append((frequency_in_seconds, AsyncCallback(callback)))

    def _sigint_sigterm_handler(self, sig, frame):
        if self.terminated_externally:
            self.logger.warning("Lifecycle: Graceful keeper termination due to SIGINT/SIGTERM already in progress")
        else:
            self.logger.warning("Lifecycle: Keeper received SIGINT/SIGTERM signal, will terminate gracefully")
            self.terminated_externally = True

    def _start_watching_blocks(self):
        def new_block_callback(block_data: dict):
            self._last_block_time = datetime.datetime.now(tz=pytz.UTC)
            block_hash = block_data.get('hash')
            if isinstance(block_hash, HexBytes):
                block_hash = block_hash.hex()
            block_number = block_data.get('number')
            if isinstance(block_number, str):
                block_number = int(block_number, 16)

            try:
                def on_start():
                    self.logger.debug(f"Lifecycle: Processing block #{block_number} ({block_hash})")

                def on_finish():
                    self.logger.debug(f"Lifecycle: Finished processing block #{block_number} ({block_hash})")

                if not self.terminated_internally and not self.terminated_externally and not self.fatal_termination:
                    if not self._on_block_callback.trigger(on_start, on_finish, block_data):
                        self.logger.debug(f"Lifecycle: Ignoring block #{block_number} ({block_hash}), as previous callback is still running")
                        # self._on_block_callback.wait()
                else:
                    self.logger.debug(f"Lifecycle: Ignoring block #{block_number} as keeper is already terminating")
            except Exception as err:
                self.logger.warning(f"Lifecycle: Ignoring block #{block_number} ({block_hash}), as error: {err} occurred.")
                msg = ""
                for t in traceback.format_tb(err.__traceback__):
                    t = t.replace("\n", ":")
                    t = t[:-1]
                    msg += f"     {t}\n"
                self.logger.info(f"{msg}")

        def new_block_watch():
            event_filter = self.web3.eth.filter('latest')
            self.logger.debug(f"Lifecycle: Created event filter: {event_filter}")
            while True:
                if self.terminated_internally or self.terminated_externally:
                    break

                try:
                    for event in event_filter.get_new_entries():
                        block_hash = 'latest'
                        new_block_callback({'hash': block_hash})
                        # skip all other-older blocks and use latest
                        break
                except (BlockNotFound, BlockNumberOutofRange, ValueError) as ex:
                    # print(f"Node dropped event emitter; recreating latest block filter: {type(ex)}: {ex}")
                    self.logger.warning(f"Lifecycle: Node dropped event emitter; recreating latest block filter: {type(ex)}: {ex}")
                    event_filter = self.web3.eth.filter('latest')
                    time.sleep(0.5)
                except Exception as err:
                    self.logger.error(f"Lifecycle: Exception: {err}")
                    self.terminated_internally = True
                    break

                    # self.logger.warning(f"Node dropped event emitter; recreating latest block filter: {err}")
                    # event_filter = self.web3.eth.filter('latest')
                finally:
                    time.sleep(0.05)

        def new_block_watch_subscribe():
            self.logger.info(f"Lifecycle: new_block_watch_subscribe started")

            async def get_event():

                if hasattr(self.web3.provider, 'endpoint_uri'):
                    endpoint_uri = self.web3.provider.endpoint_uri
                else:
                    self.logger.error(f"Lifecycle Error: invalid web3 provider: {repr(self.web3.provider)}")
                    self.terminated_internally = True
                    return

                self.logger.info(f"Lifecycle: connecting to: {endpoint_uri}")
                call_timeout = 60
                async for w3ws in AsyncWeb3.persistent_websocket(WebsocketProviderV2(endpoint_uri, call_timeout=call_timeout, websocket_kwargs={"open_timeout": call_timeout, "close_timeout": call_timeout, "ping_timeout": call_timeout})):
                    if self.terminated_internally or self.terminated_externally:
                        self.logger.warning(f"Lifecycle: terminated internally: {self.terminated_internally} or externally: {self.terminated_externally}")
                        break
                    try:
                        if not await asyncio.wait_for(w3ws.is_connected(), timeout=call_timeout):
                            self.logger.info(f"Lifecycle: connecting provider to {endpoint_uri}")
                            await w3ws.provider.connect()
                        subscription_id = await w3ws.eth.subscribe("newHeads")
                        self.logger.info(f"Lifecycle: subscribed to newHeads. Subscription id: {subscription_id}")
                    except asyncio.exceptions.TimeoutError as err:
                        self.logger.error(f"Lifecycle: timeout reached: {endpoint_uri}.")
                        time.sleep(0.5)
                        break
                    except Exception as err:
                        self.logger.error(f"Lifecycle: Exception: {err}")
                        msg = ""
                        for t in traceback.format_tb(err.__traceback__):
                            t = t.replace("\n", ":")
                            t = t[:-1]
                            msg += f"     {t}\n"
                        self.logger.info(f"{msg}")
                        self.terminated_internally = True

                    while True:
                        if self.terminated_internally or self.terminated_externally:
                            self.logger.warning(f"Lifecycle: terminated internally: {self.terminated_internally} or externally: {self.terminated_externally}")
                            break

                        try:
                            async for response in w3ws.listen_to_websocket():
                                new_block_callback(dict(response))
                        except asyncio.exceptions.TimeoutError as err:
                            self.logger.warning(f"Lifecycle: timeout: {err}.")
                        except (BlockNotFound, BlockNumberOutofRange, ValueError) as ex:
                            self.logger.warning(f"Lifecycle: Node dropped event emitter; resubscribing: {type(ex)}: {ex}")
                            time.sleep(0.5)
                            break
                        except Exception as err:
                            self.logger.error(f"Lifecycle: Exception: {err}")
                            msg = ""
                            for t in traceback.format_tb(err.__traceback__):
                                t = t.replace("\n", ":")
                                t = t[:-1]
                                msg += f"     {t}\n"
                            self.logger.info(f"{msg}")
                            self.terminated_internally = True
                            break
                    self.logger.info(f"Lifecycle: subscribe newHeads finished.")
                self.logger.info(f"Lifecycle: get_event loop finished.")

            asyncio.new_event_loop().run_until_complete(get_event())
            self.logger.info(f"Lifecycle: new_block_watch_subscribe finished.")

        if self.block_function:
            self._on_block_callback = AsyncCallback(self.block_function)

            if self.subscribe_new_heads:
                block_watch_function = new_block_watch_subscribe
            else:
                block_watch_function = new_block_watch

            block_filter = threading.Thread(target=block_watch_function, daemon=True)
            block_filter.start()
            register_filter_thread(block_filter)

            self.logger.info("Lifecycle: Watching for new blocks")

    def _start_thread_safely(self, t: threading.Thread):
        delay = 10

        while True:
            try:
                t.start()
                break
            except Exception as e:
                self.logger.critical(f"Lifecycle: Failed to start a thread ({e}), trying again in {delay} seconds")
                time.sleep(delay)

    def _start_every_timers(self):
        for idx, timer in enumerate(self.every_timers, start=1):
            self._start_every_timer(idx, timer[0], timer[1])

        for idx, event_timer in enumerate(self.event_timers, start=1):
            self._start_event_timer(idx, event_timer[0], event_timer[1], event_timer[2])

        if len(self.every_timers) > 0:
            self.logger.info(f"Lifecycle: Started {len(self.every_timers)} timer(s)")

        if len(self.event_timers) > 0:
            self.logger.info(f"Lifecycle: Started {len(self.event_timers)} event(s)")

    def _start_every_timer(self, idx: int, frequency_in_seconds: int, callback):
        def setup_timer(delay):
            timer = threading.Timer(delay, func)
            timer.daemon = True

            self._start_thread_safely(timer)

        def func():
            try:
                if not self.terminated_internally and not self.terminated_externally and not self.fatal_termination:
                    def on_start():
                        self.logger.debug(f"Lifecycle: Processing the timer #{idx}")

                    def on_finish():
                        self.logger.debug(f"Lifecycle: Finished processing the timer #{idx}")

                    if not callback.trigger(on_start, on_finish):
                        self.logger.debug(f"Lifecycle: Ignoring timer #{idx} as previous one is already running")
                else:
                    self.logger.debug(f"Lifecycle: Ignoring timer #{idx} as keeper is already terminating")
            except:
                setup_timer(frequency_in_seconds)
                raise
            setup_timer(frequency_in_seconds)

        setup_timer(1)
        self._at_least_one_every = True

    def _start_event_timer(self, idx: int, event: threading.Event, min_frequency_in_seconds: int, callback):
        def setup_thread():
            self._start_thread_safely(threading.Thread(target=func, daemon=True))

        def func():
            event_happened = False

            while True:
                try:
                    if not self.terminated_internally and not self.terminated_externally and not self.fatal_termination:
                        def on_start():
                            self.logger.debug(f"Lifecycle: Processing the event #{idx}" if event_happened
                                              else f"Processing the event #{idx} because of minimum frequency")

                        def on_finish():
                            self.logger.debug(f"Lifecycle: Finished processing the event #{idx}" if event_happened
                                              else f"Finished processing the event #{idx} because of minimum frequency")

                        assert callback.trigger(on_start, on_finish)
                        callback.wait()

                    else:
                        self.logger.debug(f"Lifecycle: Ignoring event #{idx} as keeper is terminating" if event_happened
                                          else f"Ignoring event #{idx} because of minimum frequency as keeper is terminating")
                except:
                    setup_thread()
                    raise

                event_happened = event.wait(timeout=min_frequency_in_seconds)
                event.clear()

        setup_thread()
        self._at_least_one_every = True

    def _main_loop(self):
        # terminate gracefully on either SIGINT or SIGTERM
        # signal.signal(signal.SIGINT, self._sigint_sigterm_handler)
        # signal.signal(signal.SIGTERM, self._sigint_sigterm_handler)

        # in case at least one filter has been set up, we enter an infinite loop and let
        # the callbacks do the job. in case of no filters, we will not enter this loop
        # and the keeper will terminate soon after it started
        while any_filter_thread_present() or self._at_least_one_every:
            time.sleep(1)

            # if the keeper logic asked us to terminate, we do so
            if self.terminated_internally:
                self.logger.warning("Lifecycle: Keeper logic asked for termination, the keeper will terminate")
                break

            # if SIGINT/SIGTERM asked us to terminate, we do so
            if self.terminated_externally:
                self.logger.warning("Lifecycle: The keeper is terminating due do SIGINT/SIGTERM signal received")
                break

            # if any exception is raised in filter handling thread (could be an HTTP exception
            # while communicating with the node), web3.py does not retry and the filter becomes
            # dysfunctional i.e. no new callbacks will ever be fired. we detect it and terminate
            # the keeper so it can be restarted.
            if not all_filter_threads_alive():
                self.logger.fatal("Lifecycle: One of filter threads is dead, the keeper will terminate")
                self.fatal_termination = True
                break

            # if we are watching for new blocks and no new block has been reported during
            # some time, we assume the watching filter died and terminate the keeper
            # so it can be restarted.
            #
            # this used to happen when the machine that has the node and the keeper running
            # was put to sleep and then woken up.
            #
            # TODO the same thing could possibly happen if we watch any event other than
            # TODO a new block. if that happens, we have no reliable way of detecting it now.
            if self._last_block_time and (datetime.datetime.now(tz=pytz.UTC) - self._last_block_time).total_seconds() > 300:
                if self.skip_syncing_check:
                    is_syncing = False
                else:
                    is_syncing = self.web3.eth.syncing

                if not is_syncing:
                    self.logger.fatal("Lifecycle: No new blocks received for 300 seconds, the keeper will terminate")
                    self.fatal_termination = True
                    break
        self.logger.warning("Lifecycle: Keeper logic ended main loop")
