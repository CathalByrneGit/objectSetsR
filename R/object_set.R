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
  props <- ontologySpecR::object_property_ids(object_type)
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
  
  object_type <- os_type_def(os)
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
  object_type <- os_type_def(os)
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

  keys <- ontologySpecR::link_join_keys(link)
  validate_property_ids(source_type, keys$from)
  validate_property_ids(target_type, keys$to)

  by <- stats::setNames(keys$to, keys$from)

  joined <- dplyr::inner_join(
    os$tbl,
    target_tbl,
    by = by,
    suffix = c(".from", ".to"),
    keep = TRUE
  )
  
  target_props <- ontologySpecR::object_property_ids(target_type)
  
  # Resolve and select the target columns from the joined result
  selections <- resolve_target_columns(joined, target_props)
  tbl <- dplyr::select(joined, !!!selections)
  
  os_new(os$ctx, target_type$id, tbl, target_props)
}

#' Multi-hop traversal using DuckPGQ
#'
#' Follows a link type 1 to N hops from the current object set and returns
#' every object of the destination type reachable within that range. The
#' traversal runs inside DuckDB through the DuckPGQ extension, via
#' \pkg{vertexR}.
#'
#' Use \code{os_traverse()} for a single hop: it is a plain SQL join that works
#' on any DBI backend. Use \code{os_traverse_deep()} for 1-N hops; it needs a
#' DuckDB connection, the DuckPGQ extension, and the \pkg{vertexR} package
#' (a version providing \code{vx_pgq_neighbors()}).
#'
#' If the bundle's property graph does not exist yet it is created with
#' \code{vertexR::vx_pgq_setup()}, which installs/loads DuckPGQ and creates (or
#' replaces) a property graph, plus views for query-backed object types, in the
#' connected database. A message is emitted when that happens. The graph is
#' built from the whole bundle, and DuckPGQ supports single-column primary keys
#' only.
#'
#' vertexR does not report hop counts for quantified paths, so there is no
#' \code{path_length} column.
#'
#' @param os An \code{ObjectSet}.
#' @param link_type_id Character. The link type to traverse.
#' @param min_hops Integer. Minimum number of hops. Default 1.
#' @param max_hops Integer. Maximum number of hops. Default 3.
#' @param direction One of \code{"forward"} (from the link's source type to its
#'   target type), \code{"reverse"} (the other way), or \code{"both"} (either
#'   way; the result is the type at the other end of the link).
#'
#' @return An \code{ObjectSet} of the destination type.
#' @export
os_traverse_deep <- function(os, link_type_id,
                             min_hops = 1L, max_hops = 3L,
                             direction = c("forward", "reverse", "both")) {
  ensure_object_set(os)
  direction <- match.arg(direction)
  min_hops <- as.integer(min_hops)
  max_hops <- as.integer(max_hops)
  if (length(min_hops) != 1L || is.na(min_hops) || min_hops < 1L) {
    rlang::abort("`min_hops` must be a single integer >= 1.")
  }
  if (length(max_hops) != 1L || is.na(max_hops) || max_hops < min_hops) {
    rlang::abort("`max_hops` must be a single integer >= `min_hops`.")
  }

  # Which type is at the other end of the link?
  link <- get_link_type(os$ctx, link_type_id)
  ends <- ontologySpecR::link_endpoints(link)
  source_type_id <- os$object_type_id
  target_type_id <- switch(
    direction,
    forward = {
      if (ends$from != source_type_id) {
        rlang::abort(paste0("Link ", link$id, " does not originate from ", source_type_id, "."))
      }
      ends$to
    },
    reverse = {
      if (ends$to != source_type_id) {
        rlang::abort(paste0("Link ", link$id, " does not point to ", source_type_id, "."))
      }
      ends$from
    },
    both = {
      if (source_type_id == ends$from) {
        ends$to
      } else if (source_type_id == ends$to) {
        ends$from
      } else {
        rlang::abort(paste0("Link ", link$id, " does not touch ", source_type_id, "."))
      }
    }
  )

  source_type <- get_object_type(os$ctx, source_type_id)
  target_type <- get_object_type(os$ctx, target_type_id)
  source_pk <- ontologySpecR::object_pk(source_type)
  target_pk <- ontologySpecR::object_pk(target_type)
  if (length(source_pk) != 1L || length(target_pk) != 1L) {
    rlang::abort("os_traverse_deep() supports single-column primary keys only (DuckPGQ backend).")
  }

  con <- os$ctx$connection
  if (!is_duckdb_connection(con)) {
    rlang::abort(
      "os_traverse_deep() requires a DuckDB connection. Use os_traverse() for single-hop traversal on other backends."
    )
  }
  available <- vertexr_fn("vx_pgq_available")
  graph_name_fn <- vertexr_fn("vx_pgq_graph_name")
  graph_exists <- vertexr_fn("vx_pgq_graph_exists")
  setup <- vertexr_fn("vx_pgq_setup")
  neighbors <- vertexr_fn("vx_pgq_neighbors")

  if (!isTRUE(available(con))) {
    rlang::abort(
      "os_traverse_deep() requires DuckPGQ. Use os_traverse() for single-hop traversal or install DuckPGQ."
    )
  }

  graph_name <- graph_name_fn(os$ctx$spec)
  if (!isTRUE(graph_exists(con, graph_name))) {
    rlang::inform(sprintf("Property graph '%s' not found; creating it from the bundle.", graph_name))
    setup(os$ctx$spec, con, graph_name = graph_name)
  }

  empty_target <- function() {
    tbl <- dplyr::filter(build_object_tbl(os$ctx, target_type), FALSE)
    os_new(os$ctx, target_type_id, tbl, ontologySpecR::object_property_ids(target_type))
  }

  # Seeds: the current set's primary keys. Collected (not pushed down) because
  # vertexR takes them as a vector.
  seeds <- dplyr::collect(dplyr::select(os$tbl, dplyr::all_of(source_pk)))[[source_pk]]
  seed_ids <- unique(as.character(seeds[!is.na(seeds)]))
  if (length(seed_ids) == 0L) {
    return(empty_target())
  }

  # vertexR's graph exposes each primary key as `node_id` (see vx_pgq_setup()),
  # so source_pk / target_pk are left at vertexR's default.
  found <- neighbors(
    connection = con,
    graph_name = graph_name,
    source_type = source_type_id,
    source_ids = seed_ids,
    link_type = link_type_id,
    target_type = target_type_id,
    depth = c(min_hops, max_hops),
    direction = direction
  )
  target_ids <- unique(found$target_id)
  if (length(target_ids) == 0L) {
    return(empty_target())
  }

  os_filter(object_set(os$ctx, target_type_id), !!rlang::sym(target_pk) %in% !!target_ids)
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
  keys <- ontologySpecR::link_join_keys(link)
  by <- stats::setNames(keys$from, keys$to)
  joined <- dplyr::inner_join(os$tbl, source_tbl, by = by, suffix = c(".to", ".from"), keep = TRUE)
  source_props <- ontologySpecR::object_property_ids(source_type)
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

#' Take a stable page of an object set
#'
#' Returns rows \code{offset + 1} to \code{offset + n} of an object set. The
#' result stays lazy: paging runs in the database, by numbering rows with a
#' \code{row_number()} window and filtering on it (dbplyr has no direct
#' \code{OFFSET}).
#'
#' Pages must be stable, so rows are numbered in a fixed order: the set's own
#' order if it was sorted with \code{os_arrange()}, otherwise the object type's
#' primary key. If the set has lost its primary-key columns (for example after
#' \code{os_select()} or \code{os_aggregate()}), all of its columns are used.
#'
#' @param os An \code{ObjectSet}.
#' @param offset Non-negative integer. Number of rows to skip. Default 0.
#' @param n Non-negative integer. Maximum number of rows to return.
#'
#' @return An \code{ObjectSet}, sorted in the paging order.
#' @export
os_slice <- function(os, offset = 0L, n) {
  ensure_object_set(os)
  offset <- check_count_arg(offset, "offset")
  n <- check_count_arg(n, "n")

  order_quos <- slice_order(os)
  tbl <- dbplyr::window_order(os$tbl, !!!order_quos)
  tbl <- dplyr::mutate(tbl, .os_row = dplyr::row_number())
  lo <- offset
  hi <- offset + n
  row <- rlang::sym(".os_row")
  tbl <- dplyr::filter(tbl, !!row > !!lo, !!row <= !!hi)
  tbl <- dplyr::arrange(tbl, !!row)
  tbl <- dplyr::select(tbl, -".os_row")
  os_new(os$ctx, os$object_type_id, tbl, os$properties)
}

# The order os_slice() numbers rows in: an existing os_arrange() order, else the
# primary key, else every column.
slice_order <- function(os) {
  existing <- dbplyr::op_sort(os$tbl)
  if (length(existing) > 0) {
    return(existing)
  }
  vars <- dplyr::tbl_vars(os$tbl)
  obj <- os$ctx$object_types[[os$object_type_id]]
  pk <- if (!is.null(obj)) {
    tryCatch(ontologySpecR::object_pk(obj), error = function(e) character(0))
  } else {
    character(0)
  }
  cols <- if (length(pk) > 0 && all(pk %in% vars)) pk else vars
  rlang::syms(cols)
}

check_count_arg <- function(x, arg) {
  if (length(x) != 1L || is.na(x) || !is.numeric(x) || x < 0 || x != trunc(x)) {
    rlang::abort(paste0("`", arg, "` must be a single non-negative integer."))
  }
  as.integer(x)
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
  
  object_type <- os_type_def(os)
  
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
#' Nodes are the rows of the requested object types, keyed
#' \code{"<ObjectType>::<primary key>"} so identical key values on different
#' types stay distinct. Each link type contributes one edge per joined row,
#' running from the source object (\code{link$from}) to the target object
#' (\code{link$to}), both identified by their primary keys.
#'
#' Edges whose source or target type is not in \code{object_type_ids} are
#' dropped, with one warning per link naming how many.
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
    pk_cols <- ontologySpecR::object_pk(object_type)
    nodes <- dplyr::select(build_object_tbl(ctx, object_type), dplyr::all_of(pk_cols))
    nodes <- dplyr::collect(nodes)
    node_frames[[object_type_id]] <- dplyr::mutate(
      nodes,
      .object_type = object_type_id,
      .node_key = graph_node_key(object_type_id, nodes[pk_cols])
    )
  }
  nodes_df <- dplyr::bind_rows(node_frames)
  node_index <- stats::setNames(seq_len(nrow(nodes_df)), nodes_df$.node_key)

  edge_frames <- list()
  for (link_type_id in link_type_ids) {
    link <- get_link_type(ctx, link_type_id)
    ends <- ontologySpecR::link_endpoints(link)
    from_type <- get_object_type(ctx, ends$from)
    to_type <- get_object_type(ctx, ends$to)
    keys <- ontologySpecR::link_join_keys(link)
    validate_property_ids(from_type, keys$from)
    validate_property_ids(to_type, keys$to)
    from_pk <- ontologySpecR::object_pk(from_type)
    to_pk <- ontologySpecR::object_pk(to_type)

    # Prefix every selected column by side (.f_ / .t_) so the two tables can
    # never clash, whatever the join columns are called.
    side_select <- function(object_type, cols, prefix) {
      cols <- unique(cols)
      dplyr::select(
        build_object_tbl(ctx, object_type),
        dplyr::all_of(stats::setNames(cols, paste0(prefix, cols)))
      )
    }
    joined <- dplyr::inner_join(
      side_select(from_type, c(from_pk, keys$from), ".f_"),
      side_select(to_type, c(to_pk, keys$to), ".t_"),
      by = stats::setNames(paste0(".t_", keys$to), paste0(".f_", keys$from)),
      keep = TRUE
    )
    edge_df <- dplyr::collect(joined)

    # Endpoints are the objects themselves (their primary keys), not the join
    # column values.
    from_key <- graph_node_key(ends$from, edge_df[paste0(".f_", from_pk)])
    to_key <- graph_node_key(ends$to, edge_df[paste0(".t_", to_pk)])
    edges <- data.frame(
      from = unname(node_index[from_key]),
      to = unname(node_index[to_key]),
      link_type = rep(link_type_id, length(from_key)),
      stringsAsFactors = FALSE
    )
    dangling <- is.na(edges$from) | is.na(edges$to)
    if (any(dangling)) {
      rlang::warn(sprintf(
        "Link '%s': dropped %d edge%s whose endpoint type (%s -> %s) is not in `object_type_ids`.",
        link_type_id, sum(dangling), if (sum(dangling) == 1) "" else "s",
        ends$from, ends$to
      ))
      edges <- edges[!dangling, , drop = FALSE]
    }
    edge_frames[[link_type_id]] <- edges
  }
  edges_df <- dplyr::bind_rows(edge_frames)
  tidygraph::tbl_graph(nodes = nodes_df, edges = edges_df, directed = TRUE)
}
