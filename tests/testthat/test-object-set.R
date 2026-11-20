skip_if_not_installed("duckdb")

library(DBI)

# Fixtures (bundles built with ontologySpecR constructors, DuckDB setup) live in
# helper-fixtures.R.

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

# ---- Bundle handling (ontologySpecR owns the layout) ----

test_that("context keeps the original bundle and a normalised spec bundle", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  b <- make_bundle()
  ctx <- ontology_context(b, con)
  expect_identical(ctx$bundle, b)
  expect_true(ontologySpecR::is_bundle(ctx$spec))
  expect_setequal(names(ctx$object_types), c("Airport", "FlightRoute"))
  expect_setequal(names(ctx$link_types), c("RouteOrigin", "RouteDestination"))
})

test_that("legacy key spellings still work through as_bundle(), with a warning", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Pre-0.2.0 layout: objectTypes / linkTypes, table_name, flat primary_key.
  legacy <- list(
    bundleId = "legacy-bundle",
    bundleVersion = "0.1.0",
    objectTypes = list(
      list(
        id = "Airport",
        table_name = "airports",
        primary_key = "airport_id",
        properties = list(
          list(id = "airport_id", type = "string"),
          list(id = "name", type = "string")
        )
      )
    ),
    linkTypes = list()
  )

  # as_bundle() only warns once per legacy key per session; reset so this test
  # does not depend on test order or on being run twice.
  warned <- ontologySpecR:::.legacy_warned
  rm(list = ls(warned, all.names = TRUE), envir = warned)

  # One warning per legacy key; collect them all rather than letting extras leak.
  warnings <- character()
  ctx <- withCallingHandlers(
    ontology_context(legacy, con),
    warning = function(w) {
      warnings <<- c(warnings, conditionMessage(w))
      invokeRestart("muffleWarning")
    }
  )
  expect_match(warnings, "Legacy key 'objectTypes'", fixed = TRUE, all = FALSE)
  expect_match(warnings, "Legacy key 'table_name'", fixed = TRUE, all = FALSE)
  result <- object_set(ctx, "Airport") |> os_collect()
  expect_equal(nrow(result), 2)
})

test_that("query-backed object types are built from their source SQL", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  irish <- ontologySpecR::object_type(
    id = "IrishAirport",
    properties = list(
      ontologySpecR::property_def("airport_id", "string", nullable = FALSE),
      ontologySpecR::property_def("name", "string")
    ),
    primary_key = "airport_id",
    source_kind = "query",
    source_sql = "SELECT airport_id, name FROM airports WHERE country = 'Ireland'"
  )
  ctx <- ontology_context(make_bundle(objects = list(irish), links = list()), con)
  result <- object_set(ctx, "IrishAirport") |> os_collect()
  expect_equal(result$airport_id, "DUB")
})

test_that("schema-qualified table sources resolve", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(
    make_bundle(
      objects = list(airport_type(source_schema = "main")),
      links = list()
    ),
    con
  )
  expect_equal(nrow(object_set(ctx, "Airport") |> os_collect()), 2)
})

test_that("property source column mappings are applied", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  mapped <- ontologySpecR::object_type(
    id = "Airport",
    properties = list(
      ontologySpecR::property_def("airport_id", "string", nullable = FALSE),
      ontologySpecR::property_def("airport_name", "string", source_column = "name")
    ),
    primary_key = "airport_id",
    source_kind = "table",
    source_table = "airports"
  )
  ctx <- ontology_context(make_bundle(objects = list(mapped), links = list()), con)
  result <- object_set(ctx, "Airport") |> os_collect()
  expect_equal(sort(result$airport_name), c("Dublin", "John F Kennedy"))
})

test_that("traversal is unaffected by vertexR being installed", {
  # os_traverse() used to carry a vertexR/DuckPGQ fast path that called
  # functions vertexR does not export, so it broke whenever vertexR was
  # installed. It is now a plain SQL join regardless.
  skip_if_not_installed("vertexR")
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_bundle(), con)
  origins <- object_set(ctx, "FlightRoute") |>
    os_traverse("RouteOrigin") |>
    os_collect()
  expect_equal(sort(origins$airport_id), c("DUB", "JFK"))
})

# ---- os_to_graph ----

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

# ---- Error paths ----

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

test_that("links without join keys are rejected", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  keyless <- ontologySpecR::link_type("Keyless", from = "FlightRoute", to = "Airport")
  ctx <- ontology_context(make_bundle(links = list(keyless)), con)
  expect_error(
    object_set(ctx, "FlightRoute") |> os_traverse("Keyless"),
    "missing join keys"
  )
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

# ---- Interface-aware object sets ----

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

  # Give FlightRoute a "name" property mapped to the route_id column
  route <- route_type(
    implements = "Named",
    extra_properties = list(
      ontologySpecR::property_def("name", "string", source_column = "route_id")
    )
  )
  bundle <- make_bundle(
    objects = list(airport_type(implements = "Named"), route),
    interfaces = list(named_interface())
  )

  ctx <- ontology_context(bundle, con)
  result <- object_set_by_interface(ctx, "Named") |>
    os_collect()
  # Should have rows from both Airport (2) and FlightRoute (2)
  expect_equal(nrow(result), 4)
  expect_equal(names(result), "name")
})

test_that("interface object sets compose with filter, select and aggregate", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_interface_bundle(), con)
  named <- object_set_by_interface(ctx, "Named")

  filtered <- named |> os_filter(name == "Dublin") |> os_collect()
  expect_equal(filtered$name, "Dublin")

  expect_equal(names(named |> os_select(name) |> os_collect()), "name")

  counted <- named |> os_aggregate(n = dplyr::n()) |> os_collect()
  expect_equal(counted$n, 2L)

  # Expressions are validated against the interface's properties.
  expect_error(os_filter(named, airport_id == "DUB"), "Unknown property")
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

  orphan <- ontologySpecR::interface_type(
    "Orphan",
    required_properties = list(ontologySpecR::property_requirement("x", "string"))
  )
  ctx <- ontology_context(make_interface_bundle(extra_interfaces = list(orphan)), con)
  expect_error(object_set_by_interface(ctx, "Orphan"), "No object types implement")
})

test_that("print.OntologyContext shows interfaces when present", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  ctx <- ontology_context(make_interface_bundle(), con)
  expect_output(print(ctx), "interface")
})

# ---- Interface validation in ontology_context() ----

# Airport claims to implement "Named" but has no `name` property.
make_noncompliant_bundle <- function(airport_props = list(
  ontologySpecR::property_def("airport_id", "string", nullable = FALSE),
  ontologySpecR::property_def("country", "string")
), interfaces = list(named_interface())) {
  airport <- ontologySpecR::object_type(
    id = "Airport",
    properties = airport_props,
    primary_key = "airport_id",
    implements = "Named",
    source_kind = "table",
    source_table = "airports"
  )
  make_bundle(objects = list(airport), links = list(), interfaces = interfaces)
}

test_that("context builds silently with compliant interface implementation", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  expect_silent(ontology_context(make_interface_bundle(), con, check_interfaces = TRUE))
})

test_that("context warns on non-compliant interface with strict=FALSE", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  expect_warning(
    ontology_context(make_noncompliant_bundle(), con, strict = FALSE),
    "missing required property"
  )
})

test_that("context aborts on non-compliant interface with strict=TRUE", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  expect_error(
    ontology_context(make_noncompliant_bundle(), con, strict = TRUE),
    "missing required property"
  )
})

test_that("context skips interface validation when check_interfaces=FALSE", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  expect_silent(
    ontology_context(make_noncompliant_bundle(), con, strict = TRUE,
                     check_interfaces = FALSE)
  )
})

test_that("check_interfaces has no effect when bundle has no interfaces", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  expect_silent(ontology_context(make_bundle(), con, check_interfaces = TRUE))
})

test_that("interface validation detects type mismatch", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  bundle <- make_noncompliant_bundle(airport_props = list(
    ontologySpecR::property_def("airport_id", "string", nullable = FALSE),
    ontologySpecR::property_def("name", "integer")  # interface requires string
  ))
  expect_warning(
    ontology_context(bundle, con, strict = FALSE),
    "has type 'integer' but interface requires 'string'"
  )
})

test_that("interface validation detects nullable mismatch", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  bundle <- make_noncompliant_bundle(
    airport_props = list(
      ontologySpecR::property_def("airport_id", "string", nullable = FALSE),
      ontologySpecR::property_def("name", "string")  # nullable by default
    ),
    interfaces = list(named_interface(nullable = FALSE))
  )
  expect_warning(
    ontology_context(bundle, con, strict = FALSE),
    "must be non-nullable"
  )
})

test_that("interface validation detects unknown interface", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  # Airport implements "Named", but the bundle defines no interfaces at all.
  bundle <- make_noncompliant_bundle(interfaces = list())
  expect_warning(
    ontology_context(bundle, con, strict = FALSE),
    "implements unknown interface"
  )
})

test_that("interface validation checks required links and actions", {
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  demanding <- ontologySpecR::interface_type(
    "Demanding",
    required_properties = list(ontologySpecR::property_requirement("name", "string")),
    required_links = list(ontologySpecR::link_requirement("NoSuchLink")),
    required_actions = "NoSuchAction"
  )
  bundle <- make_bundle(
    objects = list(airport_type(implements = "Demanding"), route_type()),
    interfaces = list(demanding)
  )
  msg <- tryCatch(
    ontology_context(bundle, con, strict = TRUE),
    error = function(e) conditionMessage(e)
  )
  expect_match(msg, "missing required link type 'NoSuchLink'", fixed = TRUE)
  expect_match(msg, "missing required action type 'NoSuchAction'", fixed = TRUE)
})
