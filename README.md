mysql-orm
=========

Simple MySQL ORM library.

Supports models, links, primary keys.

TODO 
----

* Add tests
* Write documentation
* Make code-first and migrations
* Make query grouping (e.g. ``` People.find({ name: "Vasya" }).find({ age: { $gt: 40}}).do() ``` instead of current form
* Make expression parsing (e.g. ``` People.find(u => u.name == "Vasya" ); ```)
* Change `__db??__` keys to Symbols
