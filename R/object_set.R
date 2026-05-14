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

#' Create a lazy object set by interface
#'
#' Finds all object types that implement the given interface and returns
#' their union, projected to only the interface properties. This is useful
#' for querying across heterogeneous types that share a common contract
#' (e.g., all \code{GeoLocated} objects).
#'
#' @param ctx An \code{OntologyContext}.
#' @param interface_id The interface identifier.
#'
#' @return An \code{ObjectSet} containing the union of all implementing types,
#'   projected to the interface properties.
#' @export
object_set_by_interface <- function(ctx, interface_id) {
  iface <- get_interface(ctx, interface_id)
  iface_props <- interface_property_ids(iface)
  type_ids <- find_implementing_types(ctx, interface_id)
  if (length(type_ids) == 0) {
    rlang::abort(paste0("No object types implement interface: ", interface_id))
  }
  os_list <- lapply(type_ids, function(tid) {
    os <- object_set(ctx, tid)
    prop_syms <- rlang::syms(iface_props)
    names(prop_syms) <- iface_props
    tbl <- dplyr::select(os$tbl, !!!prop_syms)
    os_new(ctx, tid, tbl, iface_props)
  })
  result <- os_list[[1]]
  if (length(os_list) > 1) {
    for (i in seq(2, length(os_list))) {
      tbl <- dplyr::union(result$tbl, os_list[[i]]$tbl)
      result <- os_new(ctx, interface_id, tbl, iface_props)
    }
  }
  # Use the interface_id as the object_type_id for the unioned set
  os_new(ctx, interface_id, result$tbl, iface_props)
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
  
  # Splice the quosures into filter():
  tbl <- dplyr::filter(os$tbl, !!!quos)
  
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
  tbl <- dplyr::select(os$tbl, !!!quos)
  props <- dplyr::tbl_vars(tbl)
  os_new(os$ctx, os$object_type_id, tbl, props)
}

as_chr_vec <- function(x, arg = deparse(substitute(x))) {
  if (is.null(x)) return(NULL)
  
  if (is.list(x)) {
    x <- unlist(x, use.names = FALSE)
  }
  
  x <- as.character(x)
  
  if (length(x) == 0L || anyNA(x)) {
    rlang::abort(paste0("`", arg, "` must be a non-empty character vector."))
  }
  
  x
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
  
  source_type <- get_object_type(os$ctx, os$object_type_id)
  target_type <- get_object_type(os$ctx, link$to)
  target_tbl <- build_object_tbl(os$ctx, target_type)

  join <- link$join %||% list()
  from_keys <- as_chr_vec(join$fromKeys %||% join$from_keys, "join$fromKeys")
  to_keys   <- as_chr_vec(join$toKeys   %||% join$to_keys,   "join$toKeys")

  validate_property_ids(source_type, from_keys)
  validate_property_ids(target_type, to_keys)
  
  if (is.null(from_keys) || is.null(to_keys)) {
    rlang::abort(paste0("Link ", link$id, " is missing join keys."))
  }
  
  by <- stats::setNames(to_keys, from_keys)
  
  joined <- dplyr::inner_join(
    os$tbl,
    target_tbl,
    by = by,
    suffix = c(".from", ".to"),
    keep = TRUE
  )
  
  target_props <- property_ids(target_type)
  
  # Resolve and select the target columns from the joined result
  selections <- resolve_target_columns(joined, target_props)
  tbl <- dplyr::select(joined, !!!selections)
  
  os_new(os$ctx, target_type$id, tbl, target_props)
}

#' Multi-hop traversal using DuckPGQ
#'
#' Follows a link type 1 to N hops from the current object set, returning all
#' reachable destination objects. Requires DuckPGQ to be loaded in the
#' connection (DuckDB only).
#'
#' @param os An \code{ObjectSet}.
#' @param link_type_id Character. The link type to traverse.
#' @param min_hops Integer. Minimum number of hops. Default 1.
#' @param max_hops Integer. Maximum number of hops. Default 3.
#' @param direction One of "forward", "reverse", "both". Default "forward".
#' @param include_path Logical. If TRUE, include a path_length column. Default FALSE.
#'
#' @details
#' Use \code{os_traverse()} for single-hop traversal (pure SQL JOIN, works on
#' any DBI backend). Use \code{os_traverse_deep()} for multi-hop traversal
#' (1-N hops, requires DuckPGQ/DuckDB only).
#'
#' @return An \code{ObjectSet} of the destination type containing all objects
#'   reachable within the specified hop range.
#' @export
os_traverse_deep <- function(os, link_type_id,
                              min_hops = 1L, max_hops = 3L,
                              direction = c("forward", "reverse", "both"),
                              include_path = FALSE) {
  ensure_object_set(os)
  direction <- match.arg(direction)
  min_hops <- as.integer(min_hops)
  max_hops <- as.integer(max_hops)

  if (min_hops < 1L) {
    rlang::abort("`min_hops` must be at least 1.")
  }
  if (max_hops < min_hops) {
    rlang::abort("`max_hops` must be >= `min_hops`.")
  }

  # Check for DuckDB connection
 con <- os$ctx$connection
  if (!inherits(con, "duckdb_connection")) {
    rlang::abort(
      "os_traverse_deep() requires a DuckDB connection. Use os_traverse() for single-hop traversal on other backends."
    )
  }

  # Check for vertexR package
  if (!requireNamespace("vertexR", quietly = TRUE)) {
    rlang::abort(
      "os_traverse_deep() requires the {vertexR} package. Install it with: install.packages('vertexR')"
    )
  }

  # Check DuckPGQ availability
  if (!vertexR::vx_pgq_available(con)) {
    rlang::abort(
      "os_traverse_deep() requires DuckPGQ. Use os_traverse() for single-hop traversal or install DuckPGQ."
    )
  }

  # Verify/create property graph
  graph_name <- vertexR::vx_pgq_graph_name(os$ctx$bundle)
  if (!vertexR::vx_pgq_graph_exists(con, graph_name)) {
    message(sprintf("Property graph '%s' not found. Creating it now...", graph_name))
    vertexR::vx_pgq_setup(os$ctx$bundle, con)
  }

  # Get link type and determine target
  link <- get_link_type(os$ctx, link_type_id)
  target_type_id <- switch(
    direction,
    forward = link$to,
    reverse = link$from,
    both = link$to  # For "both", we return the "to" type
  )

  # Get primary key for the source object type
  source_type <- get_object_type(os$ctx, os$object_type_id)
  source_pk <- object_primary_key(source_type)

  # Collect seed IDs from current object set
  seeds <- os |>
    os_select(!!!rlang::syms(source_pk)) |>
    os_collect()

  if (nrow(seeds) == 0) {
    # Empty seed set - return empty ObjectSet of target type
    target_type <- get_object_type(os$ctx, target_type_id)
    target_tbl <- build_object_tbl(os$ctx, target_type)
    target_tbl <- dplyr::filter(target_tbl, FALSE)
    return(os_new(os$ctx, target_type_id, target_tbl, property_ids(target_type)))
  }

  seed_ids <- if (length(source_pk) == 1) {
    seeds[[source_pk]]
  } else {
    apply(seeds, 1, function(row) paste(row, collapse = "|"))
  }

  # Call vertexR for deep traversal
  result <- vertexR::vx_pgq_neighbors(
    con = con,
    graph_name = graph_name,
    seed_ids = seed_ids,
    link_type_id = link_type_id,
    min_hops = min_hops,
    max_hops = max_hops,
    direction = direction
  )

  if (nrow(result) == 0) {
    # No results - return empty ObjectSet
    target_type <- get_object_type(os$ctx, target_type_id)
    target_tbl <- build_object_tbl(os$ctx, target_type)
    target_tbl <- dplyr::filter(target_tbl, FALSE)
    return(os_new(os$ctx, target_type_id, target_tbl, property_ids(target_type)))
  }

  # Get target type and its primary key
  target_type <- get_object_type(os$ctx, target_type_id)
  target_pk <- object_primary_key(target_type)
  target_ids <- result$target_id

  # Build ObjectSet filtered to reachable target IDs
  target_os <- object_set(os$ctx, target_type_id)

  if (length(target_pk) == 1) {
    # Single-column PK - simple IN filter
    target_os <- os_filter(target_os, !!rlang::sym(target_pk) %in% !!target_ids)
  } else {
    # Multi-column PK - need to parse composite keys
    rlang::abort("Multi-column primary keys not yet supported in os_traverse_deep().")
  }

  # Add path_length column if requested
  if (include_path && "path_length" %in% names(result)) {
    # Create a lookup table and join
    path_df <- data.frame(
      target_id = result$target_id,
      path_length = result$path_length,
      stringsAsFactors = FALSE
    )
    names(path_df)[1] <- target_pk
    path_tbl <- dplyr::copy_to(con, path_df, name = "path_lengths", overwrite = TRUE)
    target_os$tbl <- dplyr::left_join(
      target_os$tbl,
      path_tbl,
      by = target_pk
    )
    target_os$properties <- c(target_os$properties, "path_length")
  }

  target_os
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
  from_keys <- as_chr_vec(join$fromKeys %||% join$from_keys, "join$fromKeys")
  to_keys   <- as_chr_vec(join$toKeys   %||% join$to_keys,   "join$toKeys")
  if (is.null(from_keys) || is.null(to_keys)) {
    rlang::abort(paste0("Link ", link$id, " is missing join keys."))
  }
  by <- stats::setNames(from_keys, to_keys)
  joined <- dplyr::inner_join(os$tbl, source_tbl, by = by, suffix = c(".to", ".from"), keep = TRUE)
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

#' Summary expressions are evaluated lazily in a data-masking context,
#' allowing the use of helpers such as \code{dplyr::n()}.
#'
#' @param os An \code{ObjectSet}.
#' @param ... Grouping variables (unnamed) and summary expressions (named).
#'   Unnamed arguments are interpreted as grouping variables.
#'   Named arguments are interpreted as summary expressions and must be
#'   valid \pkg{dplyr} summary expressions.
#' @param .by Reserved for future use. Currently ignored.
#'
#' @return An \code{ObjectSet} containing the aggregated result.
#'
#' @examples
#' \dontrun{
#' # Count routes by origin
#' routes |>
#'   os_aggregate(origin_id, n = dplyr::n())
#'
#' # Multiple summaries
#' routes |>
#'   os_aggregate(
#'     origin_id,
#'     n = dplyr::n(),
#'     avg_stops = mean(stops)
#'   )
#'
#' # No grouping (single-row result)
#' routes |>
#'   os_aggregate(n = dplyr::n())
#' }
#'
#' @export
os_aggregate <- function(os, ..., .by = NULL) {
  ensure_object_set(os)
  
  object_type <- get_object_type(os$ctx, os$object_type_id)
  
  # Capture everything
  dots <- rlang::enquos(...)
  
  if (length(dots) == 0L) {
    rlang::abort("At least one grouping or summary expression is required.")
  }
  
  # Split dots into grouping vars (unnamed) and summaries (named)
  names_ <- names(dots)
  is_group <- names_ == "" | is.na(names_)
  
  group_quos <- dots[is_group]
  summary_quos <- dots[!is_group]
  
  if (length(summary_quos) == 0L) {
    rlang::abort("At least one named summary expression is required, e.g. n = dplyr::n().")
  }
  
  validate_group_exprs(object_type, group_quos)
  validate_summary_exprs(object_type, summary_quos)
  
  tbl <- os$tbl
  
  if (length(group_quos) > 0L) {
    tbl <- dplyr::group_by(tbl, !!!group_quos)
  }
  
  tbl <- dplyr::summarise(tbl, !!!summary_quos, .groups = "drop")
  
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
    from_keys <- as_chr_vec(join$fromKeys %||% join$from_keys, "join$fromKeys")
    to_keys   <- as_chr_vec(join$toKeys   %||% join$to_keys,   "join$toKeys")
    if (is.null(from_keys) || is.null(to_keys)) {
      rlang::abort(paste0("Link ", link$id, " is missing join keys."))
    }
    from_tbl <- build_object_tbl(ctx, from_type)
    to_tbl <- build_object_tbl(ctx, to_type)
    by <- stats::setNames(to_keys, from_keys)
    joined <- dplyr::inner_join(
      dplyr::select(from_tbl, dplyr::all_of(from_keys)),
      dplyr::select(to_tbl, dplyr::all_of(to_keys)),
      by = by,
      keep = TRUE
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
