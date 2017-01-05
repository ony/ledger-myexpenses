#!/usr/bin/python3
from itertools import groupby
from functools import reduce
from collections import namedtuple
from contextlib import closing
import operator
import sqlite3
import datetime
import logging
import argparse

class Flow(namedtuple('Flow', ['amount', 'currency', 'payee', 'comment'])):
    def __format__(self, format_spec): return format(str(self), format_spec)
    def __str__(self):
        coins = self.amount
        sign = coins >= 0
        if not sign: coins = -coins
        decim, coins = coins % 100, coins // 100
        if decim != 0 or coins >= 1000: money = ['.%.02d' % (decim,)]
        else: money = []
        while True:
            part, coins = coins % 1000, coins // 1000
            if coins == 0:
                money[:0] = ['%d' % (part,)]
                break
            else: money[:0] = [',%03d' % (part,)]
        if self.currency == 'USD': money[:0] = '$'
        else: money += [' ', self.currency]
        if not sign: money[:0] = ['-']
        return ''.join(money)

    def __add__(self, other):
        assert self.comment == other.comment
        assert self.payee == other.payee
        assert self.currency == other.currency  # No support for multi-commodities right now
        return Flow(self.amount + other.amount, self.currency, None, None)

class Entry:
    __slots__ = ('when', 'payee', 'comment', 'flow')
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return "Entry(**%r)" % ({k: getattr(self, k) for k in self.__slots__},)

    def render(entry, year=None):
        when = entry.when
        block = []
        date = when.strftime('%m/%d' if year == when.year else '%Y/%m/%d')
        header = date + ' *'
        if entry.payee: header += ' ' + entry.payee
        header += '  ; time: ' + when.strftime('%H:%M')
        block.append(header)
        del header
        if entry.comment: block.append('    ; note: ' + entry.comment)
        for (acc, flows) in entry.flow.items():
            for flow in flows:
                block.append('    {:<26}  {:>16}'.format(acc, flow))
                if flow.payee: block.append('    ; payee: ' + flow.payee)
                if flow.comment: block.append('    ; note: ' + flow.comment)

        return "\n".join(block) + "\n"

def fetchiter(cursor):
    while True:
        records = cursor.fetchmany()
        if len(records) == 0: raise StopIteration
        yield from records

class Accounts:
    __slots__ = ('_assets', '_categories')
    Account = namedtuple('Account', ['label', 'currency', 'type'])

    def __init__(self, conn = None):
        self._assets = {}
        self._categories = {}
        if conn is not None:
            with closing(conn.cursor()) as c:
                self._load_assets(c)
                self._load_categories(c)

    def _load_assets(self, c):
        c.execute('SELECT _id, {} FROM accounts'.format(", ".join(self.Account._fields)))
        for r in fetchiter(c):
            self._assets[r['_id']] = self.Account(*tuple(r)[1:])

    def _load_categories(self, c):
        c.execute('SELECT _id, parent_id, label FROM categories')
        for (_id, parent_id, label) in fetchiter(c):
            if parent_id == _id: parent_id = None
            self._categories[_id] = (parent_id, label)

    def _category(self, _id):
        (parent_id, label) = self._categories[_id]
        if parent_id is not None:
            label = self._category(parent_id) + ':' + label
        return label

    def category(self, _id):
        if _id is None: return 'Category:Unknown'
        label = self._category(_id)
        return label

    def asset(self, _id):
        asset = self._assets[_id]
        labels = []
        if asset.type == 'CASH':
            labels.extend(['Assets', 'Cash'])
        elif asset.type == 'BANK':
            labels.extend(['Assets', 'Bank'])
        elif asset.type == 'ASSET':
            labels.append('Assets')
        elif asset.type == 'CCARD':
            labels.extend(['Liabilities', 'CreditCard'])
        elif asset.type == 'LIABILITY':
            labels.extend(['Liabilities'])
        labels.append(asset.label)
        return ':'.join(labels)

    def asset_currency(self, _id):
        return self._assets[_id].currency

    def labels(self):
        for _id in self._assets.keys():
            yield self.asset(_id)
        for _id in self._categories.keys():
            if _id == 0: continue
            yield self.category(_id)

def fetch_entries(conn, log=logging.getLogger()):
    global accounts, payees
    parent = None  # last split parent. always preceed postings

    with closing(conn.cursor()) as c:
        c.execute('''SELECT *
                     FROM transactions
                     WHERE (transfer_peer IS NULL OR _id < transfer_peer)
                     ORDER BY date, parent_id IS NOT NULL''')
        for row in fetchiter(c):
            d = {k: row[k] for k in row.keys()}
            if log.getEffectiveLevel() <= logging.DEBUG:
                print ('; %r' % (d,))
            account_id = row['account_id']
            src = accounts.asset(account_id)
            cur = accounts.asset_currency(account_id)

            transfer_account = row['transfer_account']
            if transfer_account is None:
                assert row['transfer_peer'] is None
                cat_id = row['cat_id']
                if cat_id is None:
                    logging.warning("No expenses category for txn: %r" % (d))
                elif cat_id is 0:
                    parent = row  # remember for upcoming postings
                    continue  # skip parent split transaction
                else:
                    dst = accounts.category(cat_id)
            else:
                assert row['cat_id'] is 0 or row['cat_id'] is None
                dst = accounts.asset(transfer_account)

            date = row['date']
            when = datetime.datetime.fromtimestamp(date)
            comment = row['comment']
            payee_id = row['payee_id']
            amount = row['amount']

            parent_id = row['parent_id']
            if parent_id is not None:  # posting for split
                assert payee_id is None
                assert parent is not None
                assert parent['_id'] == parent_id
                assert parent['date'] == date  # XXX: for merge all dates should be equal in split
                assert not comment or not parent['comment']  # XXX: either one comment per split or posings with individual ones
                comment = comment or parent['comment']
                payee_id = parent['payee_id']
            else:
                parent = None  # forget split parent with first non-split transaction

            payee=None if payee_id is None else payees[payee_id]
            yield Entry(
                when=when,
                comment=comment,
                payee=payee,
                flow={
                    src: [Flow(amount, cur, None, None)],
                    dst: [Flow(-amount, cur, payee, comment)]
                })

def merge_splits(entries, log=logging.getLogger()):
    cur = None
    split = False
    def prepare():
        # TODO: get rid note duplicates in transaction and posting
        if not split: return cur
        payees = set(flow.payee for flows in cur.flow.values() for flow in flows if flow.payee)
        if cur.payee: payees.add(cur.payee)
        if len(payees) > 1:  # multi-payee
            cur.payee = None

        comments = set(flow.comment for flows in cur.flow.values() for flow in flows if flow.comment)
        if cur.comment: comments.add(cur.comment)
        if len(comments) > 1:  # multi-comment
            cur.comment = None

        keyfunc = lambda flow: (flow.amount > 0, str(flow.currency), str(flow.payee), str(flow.comment))

        for acc, flows in cur.flow.items():
            flows[:] = [reduce(operator.add, g) for _, g in groupby(sorted(flows, key=keyfunc), keyfunc)]
        return cur

    for entry in entries:
        if cur is None:  # remember first
            cur = entry
            continue
        if entry.when != cur.when:
            yield prepare()
            cur = entry
            split = False
            continue
        split = True
        for acc, flow in entry.flow.items():
            sflow = cur.flow.get(acc)
            if sflow is None: cur.flow[acc] = flow
            else: cur.flow[acc] = sflow + flow
    if cur is not None: yield prepare()

def action_ledger(conn, log=logging.getLogger()):
    year = None
    for entry in merge_splits(fetch_entries(conn, log=log)):
        when = entry.when
        if year != when.year:
            print(when.strftime('\nY%Y\n'))
            year = when.year
        print(entry.render(year=year))

## Entry

parser = argparse.ArgumentParser()
verbosity_group = parser.add_mutually_exclusive_group()
verbosity_group.add_argument('-v', '--verbose', action='count', default=0, help="produce more verbose information")
verbosity_group.add_argument('-q', '--quiet', action='store_true', default=False, help="inhibit any warnings")
action_group = parser.add_argument_group('alternative actions').add_mutually_exclusive_group()
action_group.add_argument('--accounts', action='store_true', help='list all accounts')
action_group.add_argument('--active-accounts', action='store_true', help='list all non-empty accounts')
action_group.add_argument('--payees', action='store_true', help='list all payees')
parser.add_argument('file', type=str, nargs='?', default="BACKUP", help="MyExpenses database")
args = parser.parse_args()
level = dict(enumerate([logging.WARNING, logging.INFO, logging.DEBUG])).get(args.verbose)
if level is None:
    parser.error("Too much of verbosity {}".format(args.verbose))
if args.quiet:
    level = logging.ERROR

logging.basicConfig(level=level)
log = logging.getLogger()

conn = sqlite3.connect(args.file)
conn.row_factory = sqlite3.Row
accounts = Accounts(conn)
with closing(conn.cursor()) as c:
    c.execute('SELECT _id, name FROM payee')
    payees = {r['_id']: r['name'] for r in fetchiter(c)}

if args.accounts:
    print("\n".join(accounts.labels()))
    parser.exit()
elif args.active_accounts:
    labels = []
    labels.extend(sorted([accounts.asset(_id)
                          for (_id,) in conn.execute('SELECT DISTINCT account_id FROM transactions')]))
    labels.extend(sorted([accounts.category(_id)
                          for (_id,) in conn.execute('SELECT DISTINCT cat_id FROM transactions WHERE cat_id IS NOT NULL AND cat_id != 0')]))
    print("\n".join(labels))
    parser.exit()
elif args.payees:
    print("\n".join(payees.values()))
    parser.exit()

action_ledger(conn, log=log)

# TODO: verify functionality of merge postings for split transaction
# TODO: mapping?
