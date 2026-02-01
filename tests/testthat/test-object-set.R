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
    os_aggregate(origin_id, .fns = list(n = dplyr::n()))
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
    source = ontologySpecR::source_binding(table = "airports")
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
    source = ontologySpecR::source_binding(table = "routes")
  )

  link <- ontologySpecR::link_type(
    id = "RouteOrigin",
    from = "FlightRoute",
    to = "Airport",
    join = ontologySpecR::join_def(
      from_keys = "origin_id",
      to_keys = "airport_id"
    )
  )

  b <- ontologySpecR::bundle(
    spec_version = "1.0.0",
    bundle_id = "test",
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
