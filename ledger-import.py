#!/usr/bin/python3
from collections import namedtuple
from contextlib import closing
import sqlite3
import datetime

def fmt_currency(coins, name):
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
    if name == 'USD': money[:0] = '$'
    else: money += [' ', name]
    if not sign: money[:0] = ['-']
    return ''.join(money)

def fmt_entry(entry, year = None):
    when = entry['when']
    block = []
    date = when.strftime('%m/%d' if year == when.year else '%Y/%m/%d')
    header = date + ' *'
    if entry['payee']: header += ' ' + entry['payee']
    header += '  ; time: ' + when.strftime('%H:%M')
    block.append(header)
    del header
    if entry['comment']: block.append('    ; note: ' + comment)
    for (acc, delta) in entry['flow'].items():
        block.append('    {:<26}  {:>16}'.format(acc, delta))
    return "\n".join(block) + "\n"

def fetchiter(cursor):
    while True:
        records = cursor.fetchmany()
        if len(records) == 0: raise StopIteration
        yield from records

class Accounts:
    __slots__ = ('_assets', '_categories')
    Account = namedtuple('Account', ['label', 'currency'])

    def __init__(self, conn = None):
        self._assets = {}
        self._categories = {}
        if conn is not None:
            with closing(conn.cursor()) as c:
                self._load_assets(c)
                self._load_categories(c)

    def _load_assets(self, c):
        c.execute('SELECT _id, label, currency FROM accounts')
        for (_id, label, cur) in fetchiter(c):
            self._assets[_id] = self.Account(label, cur)

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
        label = self._assets[_id].label
        return 'Assets:' + label

    def asset_currency(self, _id):
        return self._assets[_id].currency

## Entry

conn = sqlite3.connect('BACKUP')
conn.row_factory = sqlite3.Row
accounts = Accounts(conn)
with closing(conn.cursor()) as c:
    c.execute('SELECT _id, name FROM payee')
    payees = {r['_id']: r['name'] for r in fetchiter(c)}

year = None

with closing(conn.cursor()) as c:
    c.execute('''SELECT *
                 FROM transactions
                 WHERE (transfer_peer IS NULL OR _id < transfer_peer)''')
    for row in fetchiter(c):
        d = {k: row[k] for k in row.keys()}
        print ('; %r' % (d,))
        locals().update(d)
        src = accounts.asset(account_id)
        cur = accounts.asset_currency(account_id)
        if transfer_account is None:
            assert transfer_peer is None
            dst = accounts.category(cat_id)
        else:
            dst = accounts.asset(transfer_account)
        #print([src, fmt_currency(amount, cur), dst])
        if dst == '__SPLIT_TRANSACTION__': continue
        when = datetime.datetime.fromtimestamp(date)
        entry = {
            'when': when,
            'comment': comment,
            'payee': None if payee_id is None else payees[payee_id],
            'flow': {
                src: fmt_currency(amount, cur),
                dst: fmt_currency(-amount, cur)
            }
        }
        if year != when.year:
            print(when.strftime('\nY%Y\n'))
            year = when.year
        print(fmt_entry(entry, year=year))

# TODO: split transaction
# TODO: mapping?
