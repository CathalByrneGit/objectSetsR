skip_if_not_installed("duckdb")

library(DBI)

# os_traverse_deep() is exercised against fakes that carry vertexR's *real*
# signatures (vertexR spec-accessors branch). The fakes error on any argument
# vertexR does not have, so a rename on either side fails here rather than at
# runtime. A gated test at the bottom runs the real thing when DuckPGQ exists.

fake_vertexr <- function(available = TRUE, graph_exists = TRUE,
                         target_ids = character(), calls = new.env()) {
  list(
    vx_pgq_available = function(connection) available,
    vx_pgq_graph_name = function(bundle) "test_bundle",
    vx_pgq_graph_exists = function(connection, graph_name) graph_exists,
    vx_pgq_setup = function(bundle, connection, graph_name = NULL,
                            vertex_tables = NULL, edge_tables = NULL) {
      calls$setup <- list(bundle = bundle, connection = connection,
                          graph_name = graph_name)
      invisible(connection)
    },
    vx_pgq_neighbors = function(connection, graph_name,
                                source_type, source_ids, link_type, target_type,
                                depth = 1L,
                                direction = c("forward", "reverse", "both"),
                                source_pk = "node_id", target_pk = "node_id",
                                target_label = NULL, bundle = NULL) {
      calls$neighbors <- list(
        supplied = setdiff(names(match.call()), ""),
        connection = connection, graph_name = graph_name,
        source_type = source_type, source_ids = source_ids,
        link_type = link_type, target_type = target_type,
        depth = depth, direction = direction
      )
      data.frame(source_id = rep(source_ids[1], length(target_ids)),
                 target_id = target_ids, stringsAsFactors = FALSE)
    }
  )
}

mock_vertexr <- function(fakes, duckdb = TRUE, .env = parent.frame()) {
  testthat::local_mocked_bindings(
    vertexr_fn = function(name, pkg = "vertexR") fakes[[name]],
    is_duckdb_connection = function(con) duckdb,
    .package = "objectSetsR",
    .env = .env
  )
}

deep_ctx <- function(con) ontology_context(make_bundle(), con)

test_that("forward traversal passes vertexR its real arguments and returns target objects", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  calls <- new.env()
  mock_vertexr(fake_vertexr(target_ids = "DUB", calls = calls))

  result <- object_set(deep_ctx(con), "FlightRoute") |>
    os_traverse_deep("RouteOrigin", min_hops = 1, max_hops = 3)

  expect_s3_class(result, "ObjectSet")
  expect_equal(result$object_type_id, "Airport")
  expect_equal(os_collect(result)$airport_id, "DUB")

  a <- calls$neighbors
  # Pin the exact contract with vertexR. source_pk/target_pk are deliberately
  # left at vertexR's default ("node_id"), which is what vx_pgq_setup() exposes.
  expect_setequal(
    a$supplied,
    c("connection", "graph_name", "source_type", "source_ids", "link_type",
      "target_type", "depth", "direction")
  )
  expect_identical(a$connection, con)
  expect_equal(a$graph_name, "test_bundle")
  expect_equal(a$source_type, "FlightRoute")
  expect_equal(sort(a$source_ids), c("R1", "R2"))
  expect_equal(a$link_type, "RouteOrigin")
  expect_equal(a$target_type, "Airport")
  expect_equal(a$depth, c(1L, 3L))
  expect_equal(a$direction, "forward")
})

test_that("seeds come from the filtered object set", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  calls <- new.env()
  mock_vertexr(fake_vertexr(target_ids = "DUB", calls = calls))
  object_set(deep_ctx(con), "FlightRoute") |>
    os_filter(stops == 0L) |>
    os_traverse_deep("RouteOrigin")
  expect_equal(calls$neighbors$source_ids, "R1")
  expect_equal(calls$neighbors$depth, c(1L, 3L))
})

test_that("reverse and both pick the type at the other end of the link", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  calls <- new.env()
  mock_vertexr(fake_vertexr(target_ids = "R1", calls = calls))
  ctx <- deep_ctx(con)

  rev <- object_set(ctx, "Airport") |> os_traverse_deep("RouteOrigin", direction = "reverse")
  expect_equal(rev$object_type_id, "FlightRoute")
  expect_equal(calls$neighbors$direction, "reverse")
  expect_equal(calls$neighbors$target_type, "FlightRoute")
  expect_equal(os_collect(rev)$route_id, "R1")

  both_from_airport <- object_set(ctx, "Airport") |>
    os_traverse_deep("RouteOrigin", direction = "both")
  expect_equal(both_from_airport$object_type_id, "FlightRoute")
  expect_equal(calls$neighbors$direction, "both")

  calls$neighbors <- NULL
  mock_vertexr(fake_vertexr(target_ids = "DUB", calls = calls))
  both_from_route <- object_set(ctx, "FlightRoute") |>
    os_traverse_deep("RouteOrigin", direction = "both")
  expect_equal(both_from_route$object_type_id, "Airport")
  expect_equal(calls$neighbors$source_type, "FlightRoute")
})

test_that("direction must match the link's endpoints", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  mock_vertexr(fake_vertexr())
  ctx <- deep_ctx(con)
  expect_error(
    object_set(ctx, "Airport") |> os_traverse_deep("RouteOrigin"),
    "does not originate from"
  )
  expect_error(
    object_set(ctx, "FlightRoute") |> os_traverse_deep("RouteOrigin", direction = "reverse"),
    "does not point to"
  )
  expect_error(
    object_set(ctx, "FlightRoute") |> os_traverse_deep("Nope"),
    "Unknown link type"
  )
})

test_that("hop parameters are validated", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  os <- object_set(deep_ctx(con), "FlightRoute")
  expect_error(os_traverse_deep(os, "RouteOrigin", min_hops = 0), "min_hops")
  expect_error(os_traverse_deep(os, "RouteOrigin", min_hops = 3, max_hops = 1), "max_hops")
})

test_that("aborts on a non-DuckDB connection", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  mock_vertexr(fake_vertexr(), duckdb = FALSE)
  expect_error(
    object_set(deep_ctx(con), "FlightRoute") |> os_traverse_deep("RouteOrigin"),
    "DuckDB"
  )
})

test_that("aborts when DuckPGQ is unavailable", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  mock_vertexr(fake_vertexr(available = FALSE))
  expect_error(
    object_set(deep_ctx(con), "FlightRoute") |> os_traverse_deep("RouteOrigin"),
    "DuckPGQ"
  )
})

test_that("creates the property graph when missing, and only then", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- deep_ctx(con)

  calls <- new.env()
  mock_vertexr(fake_vertexr(graph_exists = FALSE, target_ids = "DUB", calls = calls))
  expect_message(
    object_set(ctx, "FlightRoute") |> os_traverse_deep("RouteOrigin"),
    "not found; creating it"
  )
  expect_identical(calls$setup$connection, con)
  expect_equal(calls$setup$graph_name, "test_bundle")
  expect_true(ontologySpecR::is_bundle(calls$setup$bundle))

  calls2 <- new.env()
  mock_vertexr(fake_vertexr(graph_exists = TRUE, target_ids = "DUB", calls = calls2))
  expect_no_message(
    object_set(ctx, "FlightRoute") |> os_traverse_deep("RouteOrigin")
  )
  expect_null(calls2$setup)
})

test_that("empty seeds and empty results give an empty target set", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- deep_ctx(con)

  calls <- new.env()
  mock_vertexr(fake_vertexr(target_ids = "DUB", calls = calls))
  none <- object_set(ctx, "FlightRoute") |>
    os_filter(route_id == "NOPE") |>
    os_traverse_deep("RouteOrigin")
  expect_equal(none$object_type_id, "Airport")
  expect_equal(nrow(os_collect(none)), 0)
  expect_null(calls$neighbors)  # vertexR is not called without seeds

  mock_vertexr(fake_vertexr(target_ids = character()))
  nothing <- object_set(ctx, "FlightRoute") |> os_traverse_deep("RouteOrigin")
  expect_equal(nrow(os_collect(nothing)), 0)
})

test_that("composite primary keys are rejected", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "legs", data.frame(
    flight_no = "AB1", leg_no = 1L, dest_id = "DUB", stringsAsFactors = FALSE
  ))
  leg <- ontologySpecR::object_type(
    id = "Leg",
    properties = list(
      ontologySpecR::property_def("flight_no", "string", nullable = FALSE),
      ontologySpecR::property_def("leg_no", "integer", nullable = FALSE),
      ontologySpecR::property_def("dest_id", "string")
    ),
    primary_key = c("flight_no", "leg_no"),
    source_kind = "table", source_table = "legs"
  )
  link <- ontologySpecR::link_type("LegDest", from = "Leg", to = "Airport",
                                   join_from_keys = "dest_id", join_to_keys = "airport_id")
  ctx <- ontology_context(
    make_bundle(objects = list(airport_type(), leg), links = list(link)), con
  )
  mock_vertexr(fake_vertexr())
  expect_error(
    object_set(ctx, "Leg") |> os_traverse_deep("LegDest"),
    "single-column primary keys"
  )
})

# ---- vertexr_fn() ----

test_that("vertexr_fn explains a missing package or a missing function", {
  expect_error(vertexr_fn("anything", pkg = "noSuchPackageXyz"), "requires the noSuchPackageXyz package")
  expect_error(
    vertexr_fn("no_such_function", pkg = "stats"),
    "does not provide `no_such_function()`",
    fixed = TRUE
  )
  expect_true(is.function(vertexr_fn("sd", pkg = "stats")))
})

# ---- Real DuckPGQ (skipped unless vertexR + DuckPGQ are available) ----

test_that("os_traverse_deep at one hop agrees with os_traverse on real DuckPGQ", {
  skip_if_not_installed("vertexR")
  skip_if(!exists("vx_pgq_neighbors", asNamespace("vertexR")), "vertexR lacks the DuckPGQ backend")
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  skip_if_not(isTRUE(vertexR::vx_pgq_available(con)), "DuckPGQ not available")

  ctx <- deep_ctx(con)
  single <- object_set(ctx, "FlightRoute") |> os_traverse("RouteOrigin") |> os_collect()
  deep <- suppressMessages(
    object_set(ctx, "FlightRoute") |>
      os_traverse_deep("RouteOrigin", min_hops = 1, max_hops = 1) |>
      os_collect()
  )
  expect_equal(sort(deep$airport_id), sort(unique(single$airport_id)))
})
