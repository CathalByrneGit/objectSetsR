skip_if_not_installed("duckdb")

library(DBI)

make_bundle <- function() {
  list(
    objects = list(
      list(
        id = "Airport",
        primaryKey = list(properties = list("airport_id"), strategy = "natural"),
        source = list(table = "airports"),
        properties = list(
          list(id = "airport_id", type = "string"),
          list(id = "country", type = "string"),
          list(id = "name", type = "string")
        )
      ),
      list(
        id = "FlightRoute",
        primaryKey = list(properties = list("route_id"), strategy = "natural"),
        source = list(table = "routes"),
        properties = list(
          list(id = "route_id", type = "string"),
          list(id = "origin_id", type = "string"),
          list(id = "destination_id", type = "string"),
          list(id = "stops", type = "integer")
        )
      )
    ),
    links = list(
      list(
        id = "RouteOrigin",
        from = "FlightRoute",
        to = "Airport",
        join = list(
          fromKeys = "origin_id",
          toKeys = "airport_id"
        )
      ),
      list(
        id = "RouteDestination",
        from = "FlightRoute",
        to = "Airport",
        join = list(
          fromKeys = "destination_id",
          toKeys = "airport_id"
        )
      )
    )
  )
}

setup_duckdb <- function() {
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbWriteTable(
    con,
    "airports",
    data.frame(
      airport_id = c("DUB", "JFK"),
      country = c("Ireland", "USA"),
      name = c("Dublin", "John F Kennedy"),
      stringsAsFactors = FALSE
    )
  )
  DBI::dbWriteTable(
    con,
    "routes",
    data.frame(
      route_id = c("R1", "R2"),
      origin_id = c("DUB", "JFK"),
      destination_id = c("JFK", "DUB"),
      stops = c(0L, 1L),
      stringsAsFactors = FALSE
    )
  )
  con
}

test_that("object_set supports filtering and collecting", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  routes <- object_set(ctx, "FlightRoute")
  result <- routes |>
    os_filter(stops == 0L) |>
    os_collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$route_id, "R1")
})

test_that("object_set traversal works in both directions", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  origins <- object_set(ctx, "FlightRoute") |>
    os_traverse("RouteOrigin") |>
    os_collect()
  expect_true(all(c("airport_id", "country") %in% names(origins)))
  around <- object_set(ctx, "Airport") |>
    os_search_around("RouteOrigin") |>
    os_collect()
  expect_true(all(c("route_id", "origin_id") %in% names(around)))
})

test_that("set algebra and aggregation operate on object sets", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  routes <- object_set(ctx, "FlightRoute")
  non_stop <- routes |> os_filter(stops == 0L)
  with_stop <- routes |> os_filter(stops == 1L)
  combined <- os_union(non_stop, with_stop)
  expect_equal(os_count(combined), 2)
  grouped <- routes |>
    os_aggregate(origin_id, n = dplyr::n())
  grouped_data <- os_collect(grouped)
  expect_true("n" %in% names(grouped_data))
})

test_that("os_show_query renders SQL", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  query <- object_set(ctx, "FlightRoute") |>
    os_filter(stops == 0L) |>
    os_show_query()
  expect_true(is.character(query))
})

test_that("os_select validates property names", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  os <- object_set(ctx, "Airport")
  result <- os |> os_select(airport_id, name) |> os_collect()
  expect_equal(sort(names(result)), c("airport_id", "name"))
  expect_error(os_select(os, nonexistent), "Unknown property")
})

test_that("print methods produce output", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  expect_output(print(ctx), "OntologyContext")
  os <- object_set(ctx, "Airport")
  expect_output(print(os), "ObjectSet")
})

test_that("bundle with plain string primaryKey still works", {
  bundle <- list(
    objects = list(
      list(
        id = "Airport",
        primaryKey = "airport_id",
        source = list(table = "airports"),
        properties = list(
          list(id = "airport_id", type = "string"),
          list(id = "name", type = "string")
        )
      )
    ),
    links = list()
  )
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(bundle, con)
  os <- object_set(ctx, "Airport")
  result <- os_collect(os)
  expect_equal(nrow(result), 2)
})

test_that("os_distinct removes duplicate rows", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  # Traversal can produce duplicates when multiple routes share an origin
  origins <- object_set(ctx, "FlightRoute") |>
    os_traverse("RouteOrigin") |>
    os_distinct() |>
    os_collect()
  expect_equal(nrow(origins), length(unique(origins$airport_id)))
})

test_that("os_arrange sorts results", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  sorted <- object_set(ctx, "Airport") |>
    os_arrange(name) |>
    os_collect()
  expect_equal(sorted$name, sort(sorted$name))
})

test_that("reverse traversal selects correct columns on name collision", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  # Both Airport and FlightRoute have columns; after reverse join,
  # we should get FlightRoute columns, not Airport columns
  ctx <- ontology_context(make_bundle(), con)
  around <- object_set(ctx, "Airport") |>
    os_search_around("RouteOrigin") |>
    os_collect()
  # Should have FlightRoute properties
  expect_true("route_id" %in% names(around))
  expect_true("origin_id" %in% names(around))
  expect_true("stops" %in% names(around))
  # Should NOT have Airport-only properties
  expect_false("country" %in% names(around))
})

test_that("os_to_graph builds a tidygraph from context", {
  skip_if_not_installed("tidygraph")
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  g <- os_to_graph(ctx, c("Airport", "FlightRoute"), c("RouteOrigin"))
  expect_s3_class(g, "tbl_graph")
  nodes <- tidygraph::as_tibble(g, active = "nodes")
  edges <- tidygraph::as_tibble(g, active = "edges")
  # Should have Airport + FlightRoute nodes
  expect_true("Airport" %in% nodes$.object_type)
  expect_true("FlightRoute" %in% nodes$.object_type)
  # Should have edges for RouteOrigin link

  expect_true(nrow(edges) > 0)
  expect_true("link_type" %in% names(edges))
  expect_equal(unique(edges$link_type), "RouteOrigin")
})

test_that("error on unknown object type", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  expect_error(object_set(ctx, "NonExistent"), "Unknown object type")
})

test_that("error on unknown link type", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  os <- object_set(ctx, "FlightRoute")
  expect_error(os_traverse(os, "FakeLink"), "Unknown link type")
})

test_that("os_traverse errors on wrong direction", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  airports <- object_set(ctx, "Airport")
  # RouteOrigin goes FlightRoute -> Airport, not Airport -> anything
  expect_error(os_traverse(airports, "RouteOrigin"), "does not originate from")
})

test_that("os_search_around errors on wrong direction", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  routes <- object_set(ctx, "FlightRoute")
  # RouteOrigin points TO Airport, not TO FlightRoute
  expect_error(os_search_around(routes, "RouteOrigin"), "does not point to")
})

test_that("ensure_object_set rejects non-ObjectSet", {
  expect_error(os_filter(list(), stops == 0L), "Expected an ObjectSet")
  expect_error(os_select(42, name), "Expected an ObjectSet")
  expect_error(os_collect("not_an_os"), "Expected an ObjectSet")
})

test_that("os_union and os_intersect require same object type", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  routes <- object_set(ctx, "FlightRoute")
  airports <- object_set(ctx, "Airport")
  expect_error(os_union(routes, airports), "same object type")
  expect_error(os_intersect(routes, airports), "same object type")
})

test_that("os_aggregate without grouping produces single row", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  result <- object_set(ctx, "FlightRoute") |>
    os_aggregate(n = dplyr::n()) |>
    os_collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$n, 2L)
})

test_that("os_aggregate errors without named summary", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  routes <- object_set(ctx, "FlightRoute")
  # Only unnamed (grouping) args, no named summaries
  expect_error(os_aggregate(routes, origin_id), "named summary expression")
})

test_that("os_intersect returns common rows", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  routes <- object_set(ctx, "FlightRoute")
  non_stop <- routes |> os_filter(stops == 0L)
  all_routes <- routes
  result <- os_intersect(non_stop, all_routes) |> os_collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$route_id, "R1")
})

test_that("os_filter rejects unknown property names", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  routes <- object_set(ctx, "FlightRoute")
  expect_error(os_filter(routes, fake_col == 1), "Unknown property")
})

test_that("ontologySpecR bundle objects work end-to-end", {
  skip_if_not_installed("ontologySpecR")
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  airport <- ontologySpecR::object_type(
    id = "Airport",
    primary_key = ontologySpecR::primary_key_def(properties = "airport_id"),
    properties = list(
      ontologySpecR::property_def(id = "airport_id", type = "string"),
      ontologySpecR::property_def(id = "country", type = "string"),
      ontologySpecR::property_def(id = "name", type = "string")
    ),
    source_kind = "table",
    source_table = "airports"
  )

  route <- ontologySpecR::object_type(
    id = "FlightRoute",
    primary_key = ontologySpecR::primary_key_def(properties = "route_id"),
    properties = list(
      ontologySpecR::property_def(id = "route_id", type = "string"),
      ontologySpecR::property_def(id = "origin_id", type = "string"),
      ontologySpecR::property_def(id = "destination_id", type = "string"),
      ontologySpecR::property_def(id = "stops", type = "integer")
    ),
    source_kind = "table",
    source_table = "routes"
  )

  link <- ontologySpecR::link_type(
    id = "RouteOrigin",
    from = "FlightRoute",
    to = "Airport",
    join_from_keys = "origin_id",
    join_to_keys = "airport_id"
  )

  b <- ontologySpecR::bundle(
    bundle_id = "test",
    bundle_version = "1.0.0",
    objects = list(airport, route),
    links = list(link)
  )

  ctx <- ontology_context(b, con)
  result <- object_set(ctx, "FlightRoute") |>
    os_filter(stops == 0L) |>
    os_collect()
  expect_equal(nrow(result), 1)
  expect_equal(result$route_id, "R1")

  origins <- object_set(ctx, "FlightRoute") |>
    os_traverse("RouteOrigin") |>
    os_collect()
  expect_true("airport_id" %in% names(origins))
})

# ---- Interface tests ----

make_interface_bundle <- function() {
  list(
    objects = list(
      list(
        id = "Airport",
        primaryKey = list(properties = list("airport_id"), strategy = "natural"),
        source = list(table = "airports"),
        implements = list("Named"),
        properties = list(
          list(id = "airport_id", type = "string"),
          list(id = "country", type = "string"),
          list(id = "name", type = "string")
        )
      ),
      list(
        id = "FlightRoute",
        primaryKey = list(properties = list("route_id"), strategy = "natural"),
        source = list(table = "routes"),
        properties = list(
          list(id = "route_id", type = "string"),
          list(id = "origin_id", type = "string"),
          list(id = "destination_id", type = "string"),
          list(id = "stops", type = "integer")
        )
      )
    ),
    links = list(
      list(
        id = "RouteOrigin",
        from = "FlightRoute",
        to = "Airport",
        join = list(fromKeys = "origin_id", toKeys = "airport_id")
      )
    ),
    interfaces = list(
      list(
        id = "Named",
        properties = list(
          list(id = "name", type = "string")
        )
      )
    )
  )
}

test_that("object_set_by_interface unions implementing types", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_interface_bundle(), con)

  # Only Airport implements "Named"
  result <- object_set_by_interface(ctx, "Named") |>
    os_collect()
  expect_true("name" %in% names(result))
  expect_equal(nrow(result), 2)  # 2 airports
  # Should only have interface properties, not airport_id etc.
  expect_equal(names(result), "name")
})

test_that("object_set_by_interface unions multiple implementing types", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Give FlightRoute a "name" property mapped to route_id column
  bundle <- make_interface_bundle()
  bundle$objects[[2]]$implements <- list("Named")
  bundle$objects[[2]]$properties <- c(
    bundle$objects[[2]]$properties,
    list(list(id = "name", type = "string", source = list(column = "route_id")))
  )

  ctx <- ontology_context(bundle, con)
  result <- object_set_by_interface(ctx, "Named") |>
    os_collect()
  # Should have rows from both Airport (2) and FlightRoute (2)
  expect_equal(nrow(result), 4)
  expect_equal(names(result), "name")
})

test_that("object_set_by_interface errors on unknown interface", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_interface_bundle(), con)
  expect_error(object_set_by_interface(ctx, "FakeInterface"), "Unknown interface")
})

test_that("object_set_by_interface errors when no types implement", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  bundle <- make_interface_bundle()
  # Add an interface nobody implements
  bundle$interfaces <- c(bundle$interfaces, list(
    list(id = "Orphan", properties = list(list(id = "x", type = "string")))
  ))
  ctx <- ontology_context(bundle, con)
  expect_error(object_set_by_interface(ctx, "Orphan"), "No object types implement")
})

test_that("print.OntologyContext shows interfaces when present", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_interface_bundle(), con)
  expect_output(print(ctx), "interface")
})
