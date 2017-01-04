#!/usr/bin/python3
from contextlib import closing
import sqlite3
import datetime

def fmtCurrency(coins, name):
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

def fmtEntry(entry, year = None):
    when = entry['when']
    block = []
    if year == when.year:
        block.append(when.strftime('%m/%d *  ; time: %H:%M'))
    else:
        block.append(when.strftime('%Y/%m/%d *  ; time: %H:%M'))
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
            self._assets[_id] = (label, cur)

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
        if _id is None: return 'Equity:Unknown'
        label = self._category(_id)
        return label

    def asset(self, _id):
        if _id is None: return 'Assets:Unknown'
        #try:
        label = self._assets[_id][0]
        #except KeyError:
        #    label = 'Unresolved:' + str(_id)
        return 'Assets:' + label
    def asset_currency(self, _id):
        try:
            return self._assets[_id][1]
        except KeyError:
            return 'UAH'

conn = sqlite3.connect('BACKUP')
accounts = Accounts(conn)

year = None

with closing(conn.cursor()) as c:
    c.execute('''SELECT _id, date, amount, cat_id, account_id, transfer_peer, transfer_account, payee_id, comment
                 FROM transactions
                 WHERE (transfer_peer IS NULL OR _id < transfer_peer)''')
    for row in fetchiter(c):
        #print (';' + repr(row))
        (_id, date, amount, cat_id, asset_id, transfer_peer, transfer_acc, payee_id, desc) = row
        if transfer_acc is None:
            assert transfer_peer is None
            dst = accounts.category(cat_id)
            src = accounts.asset(asset_id)
            cur = accounts.asset_currency(asset_id)
        else:
            #dst = accounts.asset(transfer_peer)
            dst = accounts.asset(transfer_acc)
            src = accounts.asset(asset_id)
            cur = accounts.asset_currency(asset_id)
        #print([src, fmtCurrency(amount, cur), dst])
        if dst == '__SPLIT_TRANSACTION__': continue
        when = datetime.datetime.fromtimestamp(date)
        entry = {
            'when': when,
            'flow': {
                src: fmtCurrency(amount, cur),
                dst: fmtCurrency(-amount, cur)
            }
        }
        if year != when.year:
            print(when.strftime('\nY%Y\n'))
            year = when.year
        print(fmtEntry(entry, year))

# TODO: split transaction
# TODO: mapping?
