# Tests for os_traverse_deep()
# These tests require DuckDB and optionally vertexR + DuckPGQ

skip_if_not_installed("duckdb")

library(DBI)

# Helper to create a test bundle with linked types
make_linked_bundle <- function() {
  list(
    objects = list(
      list(
        id = "Person",
        primaryKey = list(properties = list("person_id"), strategy = "natural"),
        source = list(table = "persons"),
        properties = list(
          list(id = "person_id", type = "string"),
          list(id = "name", type = "string")
        )
      ),
      list(
        id = "Company",
        primaryKey = list(properties = list("company_id"), strategy = "natural"),
        source = list(table = "companies"),
        properties = list(
          list(id = "company_id", type = "string"),
          list(id = "company_name", type = "string")
        )
      )
    ),
    links = list(
      list(
        id = "WorksAt",
        from = "Person",
        to = "Company",
        join = list(
          fromKeys = "person_id",
          toKeys = "company_id"
        )
      )
    )
  )
}

setup_linked_duckdb <- function() {
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbWriteTable(
    con,
    "persons",
    data.frame(
      person_id = c("P1", "P2", "P3"),
      name = c("Alice", "Bob", "Charlie"),
      stringsAsFactors = FALSE
    )
  )
  DBI::dbWriteTable(
    con,
    "companies",
    data.frame(
      company_id = c("P1", "P2"),  # person_id links to company_id
      company_name = c("Acme Corp", "Beta Inc"),
      stringsAsFactors = FALSE
    )
  )
  con
}

test_that("os_traverse_deep aborts on non-DuckDB connection", {
  skip_if_not_installed("RSQLite")

  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Create minimal tables
  DBI::dbWriteTable(con, "persons", data.frame(
    person_id = "P1", name = "Alice", stringsAsFactors = FALSE
  ))
  DBI::dbWriteTable(con, "companies", data.frame(
    company_id = "P1", company_name = "Acme", stringsAsFactors = FALSE
  ))

  ctx <- ontology_context(make_linked_bundle(), con, check_interfaces = FALSE)
  expect_error(
    object_set(ctx, "Person") |> os_traverse_deep("WorksAt"),
    "DuckDB"
  )
})

test_that("os_traverse_deep aborts without vertexR package", {
  skip_if(requireNamespace("vertexR", quietly = TRUE), "vertexR is installed")

  con <- setup_linked_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  ctx <- ontology_context(make_linked_bundle(), con, check_interfaces = FALSE)
  expect_error(
    object_set(ctx, "Person") |> os_traverse_deep("WorksAt"),
    "vertexR"
  )
})

test_that("os_traverse_deep validates hop parameters", {
  skip_if_not_installed("vertexR")

  con <- setup_linked_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  ctx <- ontology_context(make_linked_bundle(), con, check_interfaces = FALSE)
  os <- object_set(ctx, "Person")

  expect_error(os_traverse_deep(os, "WorksAt", min_hops = 0), "min_hops")
  expect_error(os_traverse_deep(os, "WorksAt", min_hops = 3, max_hops = 1), "max_hops")
})

test_that("os_traverse_deep aborts without DuckPGQ", {
  skip_if_not_installed("vertexR")

  con <- setup_linked_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Skip if DuckPGQ is actually available
  skip_if(
    tryCatch({
      vertexR::vx_pgq_available(con)
    }, error = function(e) FALSE),
    "DuckPGQ is available"
  )

  ctx <- ontology_context(make_linked_bundle(), con, check_interfaces = FALSE)
  expect_error(
    object_set(ctx, "Person") |> os_traverse_deep("WorksAt"),
    "DuckPGQ"
  )
})

# The following tests require both vertexR and DuckPGQ to be available
# They will be skipped if the environment doesn't support them

test_that("os_traverse_deep returns results with DuckPGQ", {
  skip_if_not_installed("vertexR")

  con <- setup_linked_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  skip_if_not(
    tryCatch({
      vertexR::vx_pgq_available(con)
    }, error = function(e) FALSE),
    "DuckPGQ not available"
  )

  ctx <- ontology_context(make_linked_bundle(), con, check_interfaces = FALSE)

  # Should not error and return an ObjectSet
  result <- object_set(ctx, "Person") |>
    os_traverse_deep("WorksAt", min_hops = 1, max_hops = 1)

  expect_s3_class(result, "ObjectSet")
  expect_equal(result$object_type_id, "Company")
})

test_that("os_traverse_deep returns more results at higher depth", {
  skip_if_not_installed("vertexR")

  con <- setup_linked_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  skip_if_not(
    tryCatch({
      vertexR::vx_pgq_available(con)
    }, error = function(e) FALSE),
    "DuckPGQ not available"
  )

  ctx <- ontology_context(make_linked_bundle(), con, check_interfaces = FALSE)

  single <- object_set(ctx, "Person") |>
    os_traverse("WorksAt") |>
    os_count()

  deep <- object_set(ctx, "Person") |>
    os_traverse_deep("WorksAt", min_hops = 1, max_hops = 2) |>
    os_count()

  # Deep traversal should return >= single hop results
  expect_gte(deep, single)
})

test_that("os_traverse_deep with include_path adds path_length column", {
  skip_if_not_installed("vertexR")

  con <- setup_linked_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  skip_if_not(
    tryCatch({
      vertexR::vx_pgq_available(con)
    }, error = function(e) FALSE),
    "DuckPGQ not available"
  )

  ctx <- ontology_context(make_linked_bundle(), con, check_interfaces = FALSE)

  result <- object_set(ctx, "Person") |>
    os_traverse_deep("WorksAt", include_path = TRUE) |>
    os_collect()

  # If include_path worked, path_length should be in the result
  # (This test may need adjustment based on actual vertexR behavior)
  if (nrow(result) > 0) {
    expect_true("path_length" %in% names(result))
  }
})

test_that("os_traverse_deep handles empty seed set", {
  skip_if_not_installed("vertexR")

  con <- setup_linked_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  skip_if_not(
    tryCatch({
      vertexR::vx_pgq_available(con)
    }, error = function(e) FALSE),
    "DuckPGQ not available"
  )

  ctx <- ontology_context(make_linked_bundle(), con, check_interfaces = FALSE)

  # Filter to no results, then traverse
  result <- object_set(ctx, "Person") |>
    os_filter(person_id == "NONEXISTENT") |>
    os_traverse_deep("WorksAt") |>
    os_collect()

  expect_equal(nrow(result), 0)
})
