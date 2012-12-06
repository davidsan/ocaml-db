# ocaml-db
==========

Exercice d'implémentation d'une base de donnée sous OCaml dont l'interface est spécifiée.


## Compilation
==============

```
$ ocamlc db.mli
$ ocamlc str.cma db.ml
$ ocamlc str.cma db.cmo db_test.ml -o db_test
```

## Test
```
$ ./db_test
```