import logging
import threading
import time

from types import SimpleNamespace

import pytest

import pymaker
from pymaker import Transact

SEND_SECONDS = 0.3


class FakeEth:
    def __init__(self, fail: bool = False):
        self.fail = fail

    def get_transaction_count(self, account, block_identifier='pending'):
        if self.fail:
            raise ConnectionError("node down")
        return 0


def fake_transact(fail_nonce: bool = False) -> SimpleNamespace:
    """Just what _interlocked_choose_nonce_and_send uses; the nonce is preset so no endpoint detection runs."""
    transact = SimpleNamespace(web3=SimpleNamespace(eth=FakeEth(fail_nonce)), nonce=1, replaced=False, tx_hashes=[],
                               logger=logging.getLogger('test_transaction_lock'))
    transact.name = lambda: 'test'
    # sign + eth_sendRawTransaction: the slow network part that used to hold one lock for all accounts
    transact._func = lambda *args, **kwargs: time.sleep(SEND_SECONDS) or '0x01'
    return transact


def send_concurrently(accounts) -> float:
    threads = [threading.Thread(target=Transact._interlocked_choose_nonce_and_send, args=(fake_transact(), account, 21000, {}))
               for account in accounts]
    start = time.monotonic()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return time.monotonic() - start


@pytest.fixture(autouse=True)
def fresh_nonces():
    pymaker.next_nonce.clear()
    pymaker.transaction_locks.clear()


def test_different_accounts_send_in_parallel():
    accounts = [f"0x{i:040x}" for i in range(1, 5)]
    assert send_concurrently(accounts) < 2 * SEND_SECONDS


def test_same_account_sends_one_after_another():
    assert send_concurrently(["0x" + "1" * 40] * 3) >= 3 * SEND_SECONDS


def test_account_lock_ignores_address_case():
    account = "0xAbCdEf0000000000000000000000000000000001"
    assert pymaker._transaction_lock(account) is pymaker._transaction_lock(account.lower())


def test_failed_first_nonce_fetch_releases_the_lock():
    account = "0x" + "2" * 40
    with pytest.raises(ConnectionError):
        Transact._interlocked_choose_nonce_and_send(fake_transact(fail_nonce=True), account, 21000, {})
    assert not pymaker._transaction_lock(account).locked()
