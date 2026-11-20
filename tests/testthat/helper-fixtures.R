# Shared fixtures. Every bundle is built with the ontologySpecR constructors;
# tests must not hand-roll bundle lists (the spec owns the layout).

airport_type <- function(implements = NULL, extra_properties = list(), ...) {
  ontologySpecR::object_type(
    id = "Airport",
    properties = c(
      list(
        ontologySpecR::property_def("airport_id", "string", nullable = FALSE),
        ontologySpecR::property_def("country", "string"),
        ontologySpecR::property_def("name", "string")
      ),
      extra_properties
    ),
    primary_key = "airport_id",
    implements = implements,
    source_kind = "table",
    source_table = "airports",
    ...
  )
}

route_type <- function(implements = NULL, extra_properties = list()) {
  ontologySpecR::object_type(
    id = "FlightRoute",
    properties = c(
      list(
        ontologySpecR::property_def("route_id", "string", nullable = FALSE),
        ontologySpecR::property_def("origin_id", "string"),
        ontologySpecR::property_def("destination_id", "string"),
        ontologySpecR::property_def("stops", "integer")
      ),
      extra_properties
    ),
    primary_key = "route_id",
    implements = implements,
    source_kind = "table",
    source_table = "routes"
  )
}

route_links <- function() {
  list(
    ontologySpecR::link_type(
      "RouteOrigin", from = "FlightRoute", to = "Airport",
      join_from_keys = "origin_id", join_to_keys = "airport_id"
    ),
    ontologySpecR::link_type(
      "RouteDestination", from = "FlightRoute", to = "Airport",
      join_from_keys = "destination_id", join_to_keys = "airport_id"
    )
  )
}

make_bundle <- function(objects = list(airport_type(), route_type()),
                        links = route_links(),
                        interfaces = list(),
                        actions = list()) {
  ontologySpecR::bundle(
    bundle_id = "test-bundle",
    bundle_version = "0.1.0",
    objects = objects,
    links = links,
    interfaces = interfaces,
    actions = actions
  )
}

named_interface <- function(nullable = TRUE, ...) {
  ontologySpecR::interface_type(
    "Named",
    required_properties = list(
      ontologySpecR::property_requirement("name", "string", nullable = nullable)
    ),
    ...
  )
}

# Airport implements the "Named" interface; FlightRoute does not.
make_interface_bundle <- function(extra_interfaces = list()) {
  make_bundle(
    objects = list(airport_type(implements = "Named"), route_type()),
    interfaces = c(list(named_interface()), extra_interfaces)
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

# ---- os_to_graph helpers ----

# A graph's edges as a sorted data frame of (from, to) .node_key pairs.
graph_edge_keys <- function(g) {
  nodes <- tidygraph::as_tibble(g, active = "nodes")
  edges <- tidygraph::as_tibble(g, active = "edges")
  out <- data.frame(
    from = nodes$.node_key[edges$from],
    to = nodes$.node_key[edges$to],
    stringsAsFactors = FALSE
  )
  out <- out[order(out$from, out$to), , drop = FALSE]
  rownames(out) <- NULL
  out
}

expected_edges <- function(from, to) {
  out <- data.frame(from = from, to = to, stringsAsFactors = FALSE)
  out <- out[order(out$from, out$to), , drop = FALSE]
  rownames(out) <- NULL
  out
}

graph_node_types <- function(g) {
  nodes <- tidygraph::as_tibble(g, active = "nodes")
  edges <- tidygraph::as_tibble(g, active = "edges")
  list(
    from = nodes$.object_type[edges$from],
    to = nodes$.object_type[edges$to]
  )
}
