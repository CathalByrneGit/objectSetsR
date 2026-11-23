# objectSetsR 0.3.2

* Schema-qualified object types (`source$schema`) are now queried as
  `dbplyr::in_schema(schema, table)`. The schema comes from
  `ontologySpecR::object_table_id()` (ontologySpecR >= 0.2.2), so an empty
  schema string is treated as none.

# objectSetsR 0.3.1

* New `os_slice(os, offset = 0L, n)` takes a stable page of an object set. It
  stays lazy (rows are numbered with a `row_number()` window and filtered in the
  database, since dbplyr has no direct `OFFSET`) and orders by the set's
  `os_arrange()` order if it has one, otherwise by the primary key.

# objectSetsR 0.3.0

* `os_traverse_deep()` is back, rewritten against vertexR's actual DuckPGQ API
  (`vx_pgq_available(connection)`, `vx_pgq_graph_name()`,
  `vx_pgq_graph_exists(connection, graph_name)`,
  `vx_pgq_setup(bundle, connection, graph_name)` and
  `vx_pgq_neighbors(connection, graph_name, source_type, source_ids,
  link_type, target_type, depth, direction)`). It follows a link 1-N hops
  (`depth = c(min_hops, max_hops)`), forward, reverse or both, and needs a
  DuckDB connection, DuckPGQ and vertexR; use `os_traverse()` for one hop on any
  backend. A missing or too-old vertexR now gives a clear error naming the
  missing function.
* There is no `include_path` / `path_length`: vertexR cannot report hop counts
  for quantified paths.
* vertexR is back in Suggests (and Remotes); testthat >= 3.1.7 is needed for the
  mocked-vertexR tests.

# objectSetsR 0.2.1

* Fixed `os_to_graph()` producing self-loops and wrong endpoints. Edges now run
  from the source object to the target object, identified by their primary
  keys, instead of being built from the join-column values on both sides.
* `os_to_graph()` node keys are namespaced by object type
  (`"<ObjectType>::<primary key>"`), so equal key values on different types are
  distinct nodes. Composite keys are joined column by column with `"|"`.
* `os_to_graph()` no longer breaks when both tables use the same column name,
  and warns once per link (naming the count) when edges are dropped because an
  endpoint type is not in `object_type_ids`, rather than producing `NA` edges.
* Property and link-join reads go through the new ontologySpecR 0.2.1
  accessors (`property_column()`, `property_expression()`,
  `object_properties()`, `link_join_keys()`); objectSetsR no longer reads
  `$source` fields on properties, and the `source$name` fallback is gone
  (`as_bundle()` maps it, with a warning).
* Requires ontologySpecR >= 0.2.1.

# objectSetsR 0.2.0

## Breaking changes

* Bundles are now normalised with `ontologySpecR::as_bundle()` and read through
  the ontologySpecR accessors (`bundle_object_types()`, `bundle_link_types()`,
  `bundle_interface_types()`, `object_pk()`, `object_table()`,
  `object_source_sql()`, `object_property_ids()`). objectSetsR no longer carries
  its own copy of the bundle layout or tolerates alternative key spellings.
  Raw lists with legacy keys (`objectTypes`, `table_name`, flat `primary_key`,
  ...) still work, but `as_bundle()` warns; a raw list must now provide the
  fields the spec constructors require (e.g. `bundleId`, `source$kind`).
* Interfaces are read in the spec's shape: `requiredProperties`,
  `requiredLinks`, `requiredActions`, and `implements` on object types.
  `object_set_by_interface()` projects to the interface's required properties.
* `os_traverse_deep()` has been removed. It was implemented entirely on
  `vertexR::vx_pgq_*()` functions that do not exist, so it failed as soon as
  vertexR was installed. vertexR is no longer in Suggests.

## Other changes

* `ctx$spec` holds the normalised `ontology_bundle`; `ctx$bundle` still holds
  what the caller passed. `ctx$bundle_list` is gone.
* Query-backed object types (`source$kind == "query"`) are built from
  `source$sql` and skip the backing-table existence check.
* Requires ontologySpecR >= 0.2.0 (`Remotes: CathalByrneGit/ontologySpecR`).

# objectSetsR 0.1.0

* Initial version.
