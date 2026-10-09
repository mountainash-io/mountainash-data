# Database recipes

Focused, independently runnable examples of physical database access. Each recipe
uses a small sales table with `id`, `region` and `total` columns: north has 120 and
south has 95. Recipes create their own inputs and never import another recipe.

## Setup

Use Python 3.12+ and follow the [development installation](../README.md#installation)
with the `sqlite` and `duckdb` extras. All recipes run offline without credentials
or external services. Run the documented commands from the repository root;
absolute script paths also work from another working directory.

Each script prints a small result and checks it with assertions. Run without
Python's `-O` flag. Databases are in memory and connections are closed before exit.

## Connections

| Recipe | Question |
|---|---|
| [Connections](connections/) | How do dialect kwargs, URLs and settings recipes open a database? |
| [Connection adoption](connection_adoption/) | How do I borrow an existing connection without taking its transaction or lifetime? |

## Tables and metadata

| Recipe | Question |
|---|---|
| [Local tables](local_tables/) | How do I create, insert and query a table with Ibis? |
| [Inspection](inspection/) | How do I inspect columns and scope table discovery to a namespace? |

## Writes and transactions

| Recipe | Question |
|---|---|
| [Mutations](mutations/) | How do I update existing rows, insert new rows and add a column? |
| [Indexes](indexes/) | How do I create, inspect and drop a supported index? |
| [Transactions](transactions/) | How do owned scopes commit and roll back, and how do scope and native state differ? |

## Beyond the local recipes

Remote databases require the matching extra, a reachable service and suitable
credentials. Backend support is operation-specific: these local examples are not
proof of equivalent behavior on every registered dialect. See the
[backend limitations](../README.md#backend-support-and-limits) and
[live-database testing guide](../TESTING.md#live-database-testing).

Authentication profiles belong to `mountainash-auth-client`. Pass them separately
to `connect(auth_profile=...)`; backend profiles describe the database target.
Logical query composition uses the returned Ibis expressions. Automatic
cross-database join planning is not part of this package.

## Verification

After installation, execute every recipe from the repository root:

```bash
for recipe in examples/*/example.py; do
  python "$recipe" || exit 1
done
```

Compare stdout with each recipe's **Expected output** block. Run the root README's
quick start separately. When adding a recipe, update this index, state any extra
requirements, check the demonstrated result and document deterministic output.
