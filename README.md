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
| `os_union(os1, os2)` | Set union |
| `os_intersect(os1, os2)` | Set intersection |
| `os_distinct(os)` | Remove duplicate rows |
| `os_arrange(os, ...)` | Sort results |
| `os_aggregate(os, ..., named_summaries)` | Group and summarize |
| `os_count(os)` | Count rows |
| `os_collect(os)` | Materialize to data frame |
| `os_show_query(os)` | Render SQL string |
| `os_to_graph(ctx, types, links)` | Materialize as tidygraph |

## Interface-aware queries

Bundles can define interfaces that multiple object types implement. Use
`object_set_by_interface()` to query across all implementing types:
        
```r
# Bundle with interfaces
bundle <- list(
  objects = list(
    list(id = "Airport", implements = list("GeoLocated"), ...),
    list(id = "City", implements = list("GeoLocated"), ...)
  ),
  interfaces = list(
    list(id = "GeoLocated", properties = list(
      list(id = "latitude", type = "float"),
      list(id = "longitude", type = "float")
    ))
  ),
  links = list(...)
)

ctx <- ontology_context(bundle, con)

# Query all GeoLocated objects (airports + cities)
all_locations <- object_set_by_interface(ctx, "GeoLocated") |>
  os_filter(latitude > 50) |>
  os_collect()
```

The result contains only the interface properties (`latitude`, `longitude`),
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

## Related packages

- `ontologySpecR` - Define ontology bundles (object types, link types, interfaces)
- `conceptR` - Concept lifecycle and evaluation
- `auditR` - Audit sampling and governance
- `objectExploreR` - Interactive Shiny UI
