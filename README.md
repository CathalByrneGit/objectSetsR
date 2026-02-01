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

origin_airports <- object_set(ctx, "FlightRoute") |>
  os_filter(stops == 0L) |>
  os_traverse("RouteOrigin") |>
  os_collect()

airport_counts <- object_set(ctx, "Airport") |>
  os_aggregate(country, .fns = list(n = dplyr::n())) |>
  os_collect()

object_set(ctx, "FlightRoute") |>
  os_traverse("RouteOrigin") |>
  os_filter(country == "Ireland") |>
  os_show_query()
```
