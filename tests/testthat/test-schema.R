skip_if_not_installed("duckdb")

test_that("a schema-qualified object type round-trips through object_set()", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(con, "CREATE SCHEMA analytics")
  DBI::dbExecute(con, "CREATE TABLE analytics.airports (airport_id TEXT, country TEXT, name TEXT)")
  DBI::dbExecute(con, "INSERT INTO analytics.airports VALUES ('A', 'IE', 'Dublin'), ('B', 'FR', 'Paris')")

  bundle <- make_bundle(
    objects = list(airport_type(source_schema = "analytics")),
    links = list()
  )
  ctx <- ontology_context(bundle, con)
  res <- object_set(ctx, "Airport") |>
    os_filter(country == "IE") |>
    os_collect()
  expect_equal(nrow(res), 1)
  expect_equal(res$airport_id, "A")
})

test_that("a missing schema-qualified table is reported", {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(con, "CREATE SCHEMA analytics")
  bundle <- make_bundle(
    objects = list(airport_type(source_schema = "analytics")),
    links = list()
  )
  expect_error(ontology_context(bundle, con), "Missing backing table")
})
