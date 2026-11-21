# objectSetsR

`objectSetsR` provides a lazy, ontology-aware query algebra for object types and
link traversals defined in `ontologySpecR` bundles. It builds composable
`ObjectSet` pipelines that compile to SQL via dbplyr and execute against DBI
backends such as DuckDB.

## Installation

```r
# install.packages("remotes")
remotes::install_github("CathalByrneGit/objectSetsR")
```

## Quick start

```r
library(ontologySpecR)
library(objectSetsR)
library(DBI)
library(duckdb)

b <- read_bundle(system.file("examples", "aviation-demo.json",
                             package = "ontologySpecR"))

con <- DBI::dbConnect(duckdb::duckdb())
# The demo binds Airport to table `airports` in schema `public`, and Airline /
# FlightRoute to `airlines` / `routes`. Create them before building the context:
DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS public")
# ...insert sample airport/airline/route data here...

ctx <- ontology_context(b, con)

# Filter and traverse links
origin_airports <- object_set(ctx, "FlightRoute") |>
  os_filter(stops == 0L) |>
  os_traverse("RouteOrigin") |>
  os_collect()

# Aggregation with grouping
airport_counts <- object_set(ctx, "FlightRoute") |>
  os_aggregate(origin_id, n = dplyr::n()) |>
  os_collect()

# View the generated SQL
object_set(ctx, "FlightRoute") |>
  os_traverse("RouteOrigin") |>
  os_filter(country == "Ireland") |>
  os_show_query()
```

## Bundles

Bundles are read through `ontologySpecR` (`as_bundle()` and its accessors);
objectSetsR keeps no copy of the bundle layout. Build bundles with the
`ontologySpecR` constructors or load them with `read_bundle()`. Raw lists with
legacy keys (`objectTypes`, `table_name`, ...) are still accepted but
`as_bundle()` warns.

## Core operations

| Function | Description |
|---|---|
| `ontology_context(bundle, con)` | Create a context from a bundle and DBI connection |
| `object_set(ctx, type_id)` | Create a lazy ObjectSet for an object type |
| `object_set_by_interface(ctx, interface_id)` | Union all types implementing an interface |
| `os_filter(os, ...)` | Add filter predicates (pushed to SQL) |
| `os_select(os, ...)` | Select specific properties |
| `os_traverse(os, link_id)` | Follow a link to target type |
| `os_search_around(os, link_id)` | Reverse traversal (target to source) |
| `os_traverse_deep(os, link_id, min_hops, max_hops)` | Multi-hop traversal (DuckDB + DuckPGQ + vertexR) |
| `os_union(os1, os2)` | Set union |
| `os_intersect(os1, os2)` | Set intersection |
| `os_distinct(os)` | Remove duplicate rows |
| `os_arrange(os, ...)` | Sort results |
| `os_aggregate(os, <groups>, name = <summary>)` | Group (unnamed) and summarize (named) |
| `os_count(os)` | Count rows |
| `os_collect(os)` | Materialize to data frame |
| `os_show_query(os)` | Render SQL string |
| `os_to_graph(ctx, types, links)` | Materialize as tidygraph |

## Multi-hop traversal

`os_traverse()` is a single SQL join and works on any DBI backend.
`os_traverse_deep()` follows a link 1-N hops inside DuckDB using DuckPGQ via
the optional `vertexR` package, and returns every reachable object of the
destination type:

```r
reachable <- object_set(ctx, "FlightRoute") |>
  os_filter(stops == 0L) |>
  os_traverse_deep("RouteOrigin", min_hops = 1, max_hops = 3)
```

If the bundle's property graph doesn't exist yet it is created with
`vertexR::vx_pgq_setup()` (a message says so). Primary keys must be single
columns.

## Interface-aware queries

Bundles can define interfaces that multiple object types implement. Use
`object_set_by_interface()` to query across all implementing types:
        
```r
library(ontologySpecR)

geo <- interface_type(
  "GeoLocated",
  required_properties = list(
    property_requirement("latitude", "number"),
    property_requirement("longitude", "number")
  )
)

# Object types opt in with `implements`; ontology_context() checks that each one
# actually provides the required properties (warning, or an error if
# `strict = TRUE`; skip with `check_interfaces = FALSE`).
bundle <- bundle(
  bundle_id = "geo-demo",
  bundle_version = "0.1.0",
  objects = list(
    object_type("Airport", implements = "GeoLocated", primary_key = "airport_id",
                properties = list(property_def("airport_id", "string", nullable = FALSE),
                                  property_def("latitude", "number"),
                                  property_def("longitude", "number")),
                source_kind = "table", source_table = "airports"),
    object_type("City", implements = "GeoLocated", primary_key = "city_id",
                properties = list(property_def("city_id", "string", nullable = FALSE),
                                  property_def("latitude", "number"),
                                  property_def("longitude", "number")),
                source_kind = "table", source_table = "cities")
  ),
  interfaces = list(geo)
)

ctx <- ontology_context(bundle, con)

# Query all GeoLocated objects (airports + cities)
all_locations <- object_set_by_interface(ctx, "GeoLocated") |>
  os_filter(latitude > 50) |>
  os_collect()
```

The result contains only the interface's required properties (`latitude`, `longitude`),
projected from all implementing types and unioned together.

## Graph materialization

Convert object sets to tidygraph for network analysis:

```r
library(tidygraph)

g <- os_to_graph(ctx, 
  object_type_ids = c("Airport", "FlightRoute"),
  link_type_ids = c("RouteOrigin", "RouteDestination")
)

# Now use tidygraph/ggraph for analysis and visualization
```

Nodes are the rows of the requested object types, keyed
`"<ObjectType>::<primary key>"` (in `.node_key`), so an Airport `1` and a
FlightRoute `1` are different nodes. Each link type adds one edge per joined row,
running from the link's source object to its target object (e.g. route ->
airport), identified by their primary keys. Edges whose source or target type
isn't in `object_type_ids` are dropped, with one warning per link.

## Related packages

- `ontologySpecR` - Define ontology bundles (object types, link types, interfaces)
- `conceptR` - Concept lifecycle and evaluation
- `auditR` - Audit sampling and governance
- `objectExploreR` - Interactive Shiny UI
