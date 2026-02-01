#' Create a lazy object set
#'
#' Constructs an \code{ObjectSet} for an ontology object type. The returned
#' object is lazy and only executes when collected.
#'
#' @param ctx An \code{OntologyContext}.
#' @param object_type_id The object type identifier.
#'
#' @return An \code{ObjectSet}.
#' @export
object_set <- function(ctx, object_type_id) {
  object_type <- get_object_type(ctx, object_type_id)
  tbl <- build_object_tbl(ctx, object_type)
  props <- property_ids(object_type)
  os_new(ctx, object_type_id, tbl, props)
}

#' Filter an object set
#'
#' @param os An \code{ObjectSet}.
#' @param ... Filter expressions evaluated against object properties.
#'
#' @return An \code{ObjectSet}.
#' @export
os_filter <- function(os, ...) {
  ensure_object_set(os)
  quos <- rlang::enquos(...)
  object_type <- get_object_type(os$ctx, os$object_type_id)
  validate_filter_exprs(object_type, quos)
  tbl <- rlang::inject(dplyr::filter(os$tbl, !!!quos))
  os_new(os$ctx, os$object_type_id, tbl, os$properties)
}

#' Select properties from an object set
#'
#' @param os An \code{ObjectSet}.
#' @param ... Selection expressions passed to \code{dplyr::select()}.
#'
#' @return An \code{ObjectSet}.
#' @export
os_select <- function(os, ...) {
  ensure_object_set(os)
  quos <- rlang::enquos(...)
  object_type <- get_object_type(os$ctx, os$object_type_id)
  symbols <- unique(unlist(lapply(quos, function(q) {
    expr <- rlang::get_expr(q)
    if (rlang::is_symbol(expr)) {
      return(rlang::as_string(expr))
    }
    character(0)
  })))
  if (length(symbols) > 0) {
    validate_property_ids(object_type, symbols)
  }
  tbl <- rlang::inject(dplyr::select(os$tbl, !!!quos))
  props <- dplyr::tbl_vars(tbl)
  os_new(os$ctx, os$object_type_id, tbl, props)
}

#' Traverse a link to a target object type
#'
#' @param os An \code{ObjectSet}.
#' @param link_type_id The link type identifier to traverse.
#'
#' @return An \code{ObjectSet} for the target object type.
#' @export
os_traverse <- function(os, link_type_id) {
  ensure_object_set(os)
  link <- get_link_type(os$ctx, link_type_id)
  if (link$from != os$object_type_id) {
    rlang::abort(paste0("Link ", link$id, " does not originate from ", os$object_type_id, "."))
  }
  target_type <- get_object_type(os$ctx, link$to)
  target_tbl <- build_object_tbl(os$ctx, target_type)
  join <- link$join %||% list()
  from_keys <- join$fromKeys %||% join$from_keys
  to_keys <- join$toKeys %||% join$to_keys
  if (is.null(from_keys) || is.null(to_keys)) {
    rlang::abort(paste0("Link ", link$id, " is missing join keys."))
  }
  by <- stats::setNames(to_keys, from_keys)
  joined <- dplyr::inner_join(os$tbl, target_tbl, by = by, suffix = c(".from", ".to"))
  # inner_join drops the right-side key columns; re-create them from the kept left-side keys
  key_aliases <- stats::setNames(rlang::syms(from_keys), to_keys)
  joined <- dplyr::mutate(joined, !!!key_aliases)
  target_props <- property_ids(target_type)
  selections <- resolve_target_columns(joined, target_props)
  tbl <- dplyr::select(joined, !!!selections)
  os_new(os$ctx, target_type$id, tbl, target_props)
}

#' Traverse a link in reverse
#'
#' @param os An \code{ObjectSet}.
#' @param link_type_id The link type identifier to traverse in reverse.
#'
#' @return An \code{ObjectSet} for the source object type.
#' @export
os_search_around <- function(os, link_type_id) {
  ensure_object_set(os)
  link <- get_link_type(os$ctx, link_type_id)
  if (link$to != os$object_type_id) {
    rlang::abort(paste0("Link ", link$id, " does not point to ", os$object_type_id, "."))
  }
  source_type <- get_object_type(os$ctx, link$from)
  source_tbl <- build_object_tbl(os$ctx, source_type)
  join <- link$join %||% list()
  from_keys <- join$fromKeys %||% join$from_keys
  to_keys <- join$toKeys %||% join$to_keys
  if (is.null(from_keys) || is.null(to_keys)) {
    rlang::abort(paste0("Link ", link$id, " is missing join keys."))
  }
  by <- stats::setNames(from_keys, to_keys)
  joined <- dplyr::inner_join(os$tbl, source_tbl, by = by, suffix = c(".to", ".from"))
  # inner_join drops the right-side key columns; re-create them from the kept left-side keys
  key_aliases <- stats::setNames(rlang::syms(to_keys), from_keys)
  joined <- dplyr::mutate(joined, !!!key_aliases)
  source_props <- property_ids(source_type)
  selections <- resolve_target_columns(joined, source_props, suffix = ".from")
  tbl <- dplyr::select(joined, !!!selections)
  os_new(os$ctx, source_type$id, tbl, source_props)
}

#' Remove duplicate rows from an object set
#'
#' @param os An \code{ObjectSet}.
#'
#' @return An \code{ObjectSet} with distinct rows.
#' @export
os_distinct <- function(os) {
  ensure_object_set(os)
  tbl <- dplyr::distinct(os$tbl)
  os_new(os$ctx, os$object_type_id, tbl, os$properties)
}

#' Sort an object set
#'
#' @param os An \code{ObjectSet}.
#' @param ... Column expressions passed to \code{dplyr::arrange()}.
#'
#' @return An \code{ObjectSet}.
#' @export
os_arrange <- function(os, ...) {
  ensure_object_set(os)
  tbl <- dplyr::arrange(os$tbl, ...)
  os_new(os$ctx, os$object_type_id, tbl, os$properties)
}

#' Union two object sets
#'
#' @param os1 An \code{ObjectSet}.
#' @param os2 An \code{ObjectSet}.
#'
#' @return An \code{ObjectSet}.
#' @export
os_union <- function(os1, os2) {
  ensure_object_set(os1)
  ensure_object_set(os2)
  ensure_same_object_type(os1, os2)
  tbl <- dplyr::union(os1$tbl, os2$tbl)
  os_new(os1$ctx, os1$object_type_id, tbl, os1$properties)
}

#' Intersect two object sets
#'
#' @param os1 An \code{ObjectSet}.
#' @param os2 An \code{ObjectSet}.
#'
#' @return An \code{ObjectSet}.
#' @export
os_intersect <- function(os1, os2) {
  ensure_object_set(os1)
  ensure_object_set(os2)
  ensure_same_object_type(os1, os2)
  tbl <- dplyr::intersect(os1$tbl, os2$tbl)
  os_new(os1$ctx, os1$object_type_id, tbl, os1$properties)
}

#' Count rows in an object set
#'
#' @param os An \code{ObjectSet}.
#'
#' @return A scalar count.
#' @export
os_count <- function(os) {
  ensure_object_set(os)
  result <- dplyr::tally(os$tbl)
  count <- dplyr::collect(result)
  count$n[[1]]
}

#' Aggregate an object set
#'
#' @param os An \code{ObjectSet}.
#' @param ... Grouping expressions.
#' @param .fns Named list of summary expressions.
#'
#' @return An aggregated \code{ObjectSet}.
#' @export
os_aggregate <- function(os, ..., .fns) {
  ensure_object_set(os)
  object_type <- get_object_type(os$ctx, os$object_type_id)
  group_quos <- rlang::enquos(...)
  validate_group_exprs(object_type, group_quos)
  if (missing(.fns)) {
    rlang::abort("`.fns` is required for aggregations.")
  }
  if (rlang::is_function(.fns)) {
    rlang::abort("`.fns` must be a named list of summary expressions.")
  }
  fns <- .fns
  if (!rlang::is_list(fns) || is.null(names(fns)) || any(names(fns) == "")) {
    rlang::abort("`.fns` must be a named list of summary expressions.")
  }
  validate_summary_exprs(object_type, fns)
  tbl <- rlang::inject(dplyr::group_by(os$tbl, !!!group_quos))
  tbl <- rlang::inject(dplyr::summarise(tbl, !!!fns, .groups = "drop"))
  os_new(os$ctx, os$object_type_id, tbl, dplyr::tbl_vars(tbl))
}

#' Collect an object set into memory
#'
#' @param os An \code{ObjectSet}.
#'
#' @return A data frame with the materialized results.
#' @export
os_collect <- function(os) {
  ensure_object_set(os)
  dplyr::collect(os$tbl)
}

#' Render the SQL for an object set
#'
#' @param os An \code{ObjectSet}.
#'
#' @return A SQL string.
#' @export
os_show_query <- function(os) {
  ensure_object_set(os)
  dbplyr::sql_render(os$tbl)
}

#' Materialize a subgraph as a tidygraph graph
#'
#' @param ctx An \code{OntologyContext}.
#' @param object_type_ids Character vector of object type identifiers.
#' @param link_type_ids Character vector of link type identifiers.
#'
#' @return A \code{tidygraph::tbl_graph}.
#' @export
os_to_graph <- function(ctx, object_type_ids, link_type_ids) {
  if (!requireNamespace("tidygraph", quietly = TRUE)) {
    rlang::abort("tidygraph is required for os_to_graph().")
  }
  node_frames <- list()
  for (object_type_id in object_type_ids) {
    object_type <- get_object_type(ctx, object_type_id)
    pk <- object_primary_key(object_type)
    pk_cols <- pk
    tbl <- build_object_tbl(ctx, object_type)
    nodes <- dplyr::select(tbl, dplyr::all_of(pk_cols))
    nodes <- dplyr::collect(nodes)
    key <- compose_node_key(nodes)
    node_frames[[object_type_id]] <- dplyr::mutate(
      nodes,
      .object_type = object_type_id,
      .node_key = key
    )
  }
  nodes_df <- dplyr::bind_rows(node_frames)
  node_index <- stats::setNames(seq_len(nrow(nodes_df)), nodes_df$.node_key)
  edge_frames <- list()
  for (link_type_id in link_type_ids) {
    link <- get_link_type(ctx, link_type_id)
    from_type <- get_object_type(ctx, link$from)
    to_type <- get_object_type(ctx, link$to)
    join <- link$join %||% list()
    from_keys <- join$fromKeys %||% join$from_keys
    to_keys <- join$toKeys %||% join$to_keys
    if (is.null(from_keys) || is.null(to_keys)) {
      rlang::abort(paste0("Link ", link$id, " is missing join keys."))
    }
    from_tbl <- build_object_tbl(ctx, from_type)
    to_tbl <- build_object_tbl(ctx, to_type)
    by <- stats::setNames(to_keys, from_keys)
    joined <- dplyr::inner_join(
      dplyr::select(from_tbl, dplyr::all_of(from_keys)),
      dplyr::select(to_tbl, dplyr::all_of(to_keys)),
      by = by
    )
    edge_df <- dplyr::collect(joined)
    from_key <- compose_node_key(edge_df[from_keys])
    to_key <- compose_node_key(edge_df[to_keys])
    edge_frames[[link_type_id]] <- data.frame(
      from = node_index[from_key],
      to = node_index[to_key],
      link_type = link_type_id,
      stringsAsFactors = FALSE
    )
  }
  edges_df <- dplyr::bind_rows(edge_frames)
  tidygraph::tbl_graph(nodes = nodes_df, edges = edges_df, directed = TRUE)
}
