ledger-myexpenses
=================

Import tool for a nice android app [MyExpenses](https://github.com/mtotschnig/MyExpenses) by [Michael Totschnig](http://michael.totschnig.org)

Features
--------
* Use category as a counter-party
* Classifies assets between cash, bank etc. I.e. accounts like
  `Assets:Cash:Wallet` where `Wallet` is the name of the book
* Support for split transactions
* Aggregation of multiple transaction within a same second as a multi-posting
  transaction

Suggested workflow
------------------

* Maintain both ledger journal and MyExpenses.
* Keep account names and payees in MyExpenses in sync with main ledger journal.
* You may want to have root categories with names like `Expenses:Food` and
  assets like `Bank:Card`.
* If you need to generate transaction with multi-way 3+ postings you can create
  them with command "Save and new" which effectively keeps exact time.
* Periodically backup data from MyExpenses and append output of this tool to
  ledger journal.
* Sort generated transaction as you like but keep `refs:...` tags intact.

Pass `--excludes` option to this tool with a path to file with full ledger
journal to avoid re-generating old transactions.
If your journal is scattered over multiple files you can use output of
`ledger print`.

Under `bash` you can use `ledger print | ./ledger_myexpenses.py -x -`.

Under `zsh` you can use `./ledger_myexpenses.py -x <(ledger print)`.
