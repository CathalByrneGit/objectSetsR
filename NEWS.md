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
