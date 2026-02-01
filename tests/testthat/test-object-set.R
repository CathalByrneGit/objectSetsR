skip_if_not_installed("duckdb")

library(DBI)

seed_bundle_data <- function(bundle_list, con) {
  objects <- bundle_list$objects
  links <- bundle_list$links
  make_sample_values <- function(type, n) {
    switch(
      type,
      string = paste0("value_", seq_len(n)),
      integer = seq_len(n),
      number = as.numeric(seq_len(n)),
      boolean = rep(c(TRUE, FALSE), length.out = n),
      date = as.Date("2020-01-01") + seq_len(n) - 1,
      datetime = as.POSIXct("2020-01-01", tz = "UTC") + seq_len(n) * 3600,
      json = rep("{}", n),
      rep(NA, n)
    )
  }
  data_by_object <- list()
  for (obj in objects) {
    props <- obj$properties
    n <- 2
    df <- data.frame(stringsAsFactors = FALSE)
    for (prop in props) {
      df[[prop$id]] <- make_sample_values(prop$type, n)
    }
    pk <- obj$primaryKey
    pk_props <- if (is.list(pk) && !is.null(pk$properties)) pk$properties else pk
    for (pk_prop in pk_props) {
      df[[pk_prop]] <- paste0(obj$id, "_", seq_len(n))
    }
    data_by_object[[obj$id]] <- df
  }
  for (link in links) {
    join <- link$join
    from_keys <- join$fromKeys
    to_keys <- join$toKeys
    from_df <- data_by_object[[link$from]]
    to_df <- data_by_object[[link$to]]
    if (length(from_keys) == length(to_keys)) {
      for (i in seq_along(from_keys)) {
        from_df[[from_keys[[i]]]] <- to_df[[to_keys[[i]]]]
      }
    }
    data_by_object[[link$from]] <- from_df
  }
  for (obj in objects) {
    table <- obj$source$table
    schema <- obj$source$schema
    table_id <- if (!is.null(schema)) DBI::Id(schema = schema, table = table) else table
    DBI::dbWriteTable(con, table_id, data_by_object[[obj$id]], overwrite = TRUE)
  }
}

make_bundle <- function() {
  list(
    objectTypes = list(
      list(
        id = "Airport",
        primaryKey = "airport_id",
        source = list(table = "airports"),
        properties = list(
          list(id = "airport_id", type = "string"),
          list(id = "country", type = "string"),
          list(id = "name", type = "string")
        )
      ),
      list(
        id = "FlightRoute",
        primaryKey = "route_id",
        source = list(table = "routes"),
        properties = list(
          list(id = "route_id", type = "string"),
          list(id = "origin_id", type = "string"),
          list(id = "destination_id", type = "string"),
          list(id = "stops", type = "integer")
        )
      )
    ),
    linkTypes = list(
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

test_that("ontologySpecR bundles work end-to-end", {
  skip_if_not_installed("ontologySpecR")
  con <- setup_duckdb()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  bundle <- ontologySpecR::read_bundle(
    system.file("examples", "aviation-demo.json", package = "ontologySpecR")
  )
  bundle_list <- ontologySpecR::as_list(bundle)
  seed_bundle_data(bundle_list, con)
  ctx <- ontology_context(bundle, con)
  object_type_id <- bundle_list$objects[[1]]$id
  result <- object_set(ctx, object_type_id) |>
    os_collect()
  expect_true(nrow(result) > 0)
})
