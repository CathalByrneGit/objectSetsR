# The bundle layout (key spellings, primary-key structure, source bindings) is
# owned by ontologySpecR. Everything in this file reads bundles through its
# accessors. `ctx$spec` is the normalised `ontology_bundle` produced by
# `ontologySpecR::as_bundle()`; `ctx$bundle` keeps whatever the caller passed.

get_interface <- function(ctx, interface_id) {
  iface <- ctx$interface_types[[interface_id]]
  if (is.null(iface)) {
    rlang::abort(paste0("Unknown interface: ", interface_id))
  }
  iface
}

interface_property_ids <- function(interface_type) {
  reqs <- interface_type$requiredProperties
  if (length(reqs) == 0) {
    rlang::abort(paste0("Interface ", interface_type$id, " has no required properties."))
  }
  vapply(reqs, `[[`, character(1), "id")
}

find_implementing_types <- function(ctx, interface_id) {
  matches <- character(0)
  for (obj in ctx$object_types) {
    if (interface_id %in% unlist(obj$implements, use.names = FALSE)) {
      matches <- c(matches, obj$id)
    }
  }
  matches
}

get_object_type <- function(ctx, object_type_id) {
  obj <- ctx$object_types[[object_type_id]]
  if (is.null(obj)) {
    rlang::abort(paste0("Unknown object type: ", object_type_id))
  }
  obj
}

get_link_type <- function(ctx, link_type_id) {
  link <- ctx$link_types[[link_type_id]]
  if (is.null(link)) {
    rlang::abort(paste0("Unknown link type: ", link_type_id))
  }
  link
}

# The type an ObjectSet is "of", for validating expressions against its
# properties. Normally an object type; sets built by object_set_by_interface()
# are of the interface, so they validate against its required properties.
os_type_def <- function(os) {
  id <- os$object_type_id
  obj <- os$ctx$object_types[[id]]
  if (!is.null(obj)) {
    return(obj)
  }
  iface <- os$ctx$interface_types[[id]]
  if (!is.null(iface)) {
    return(list(
      id = id,
      properties = lapply(iface$requiredProperties, function(r) list(id = r$id, type = r$type))
    ))
  }
  rlang::abort(paste0("Unknown object type: ", id))
}

# Where an object type's rows live: a (possibly schema-qualified) table, or the
# defining SQL for query-backed types.
object_backing <- function(object_type) {
  src <- ontologySpecR::object_source(object_type)
  if (is.null(src)) {
    rlang::abort(paste0("Object type ", object_type$id, " has no source binding."))
  }
  if (identical(src$kind, "query")) {
    sql <- ontologySpecR::object_source_sql(object_type)
    if (is.null(sql)) {
      rlang::abort(paste0("Object type ", object_type$id, " is query-backed but has no source SQL."))
    }
    return(list(kind = "query", sql = sql))
  }
  table <- ontologySpecR::object_table(object_type)
  if (!is.null(src$schema)) {
    table <- DBI::Id(schema = src$schema, table = table)
  }
  list(kind = "table", table = table)
}

validate_property_ids <- function(object_type, symbols) {
  props <- ontologySpecR::object_property_ids(object_type)
  missing <- setdiff(symbols, props)
  if (length(missing) > 0) {
    rlang::abort(
      paste0(
        "Unknown property(ies) for ", object_type$id, ": ",
        paste(missing, collapse = ", ")
      )
    )
  }
}

#' Extract symbol names from a filter expression (safe for validation)
#' @noRd
symbols_in_expr <- function(expr) {
  # Symbols like stops, distance, etc.
  if (rlang::is_symbol(expr)) {
    name <- rlang::as_string(expr)
    if (name %in% c(".data", "TRUE", "FALSE", "NULL")) return(character(0))
    return(name)
  }
  
  # Calls like stops == 0L, stops %in% c(...), is.na(stops), .data$stops, etc.
  if (rlang::is_call(expr)) {
    fn_name <- rlang::call_name(expr)
    # Special-case `.data$prop` to return just `prop`
    if (!is.null(fn_name) && fn_name == "$" && length(expr) == 3L) {
      lhs <- expr[[2]]
      rhs <- expr[[3]]
      if (rlang::is_symbol(lhs) && rlang::as_string(lhs) == ".data") {
        if (rlang::is_symbol(rhs)) return(rlang::as_string(rhs))
        if (rlang::is_string(rhs)) return(rhs)
      }
    }
    
    # Recurse into call arguments (NOT into arbitrary lists/pairlists)
    args <- as.list(expr)[-1]
    return(unique(unlist(lapply(args, symbols_in_expr), use.names = FALSE)))
  }
  
  # Everything else: constants, pairlists, expressions, lists, etc.
  character(0)
}

#' Validate that filter expressions only refer to known properties
#' @noRd
validate_filter_exprs <- function(object_type, quos) {
  # `quos` is a list of quosures (from enquos)
  symbols <- unique(unlist(
    lapply(quos, function(q) symbols_in_expr(rlang::get_expr(q))),
    use.names = FALSE
  ))
  
  # Drop operator/function symbols that can show up during traversal
  symbols <- setdiff(
    symbols,
    c(
      # infix operators / primitives
      "==", "!=", "<", "<=", ">", ">=", "&", "|", "!", "+", "-", "*", "/", "^",
      "$", "[", "[[", "(", "{", "~", ":", "::", ":::",
      # common helpers that might appear in filters
      "c", "list", "is.na", "if_else", "case_when", "between", "in", "%in%"
    )
  )
  
  validate_property_ids(object_type, symbols)
}



validate_group_exprs <- function(object_type, quos) {
  symbols <- unique(unlist(lapply(quos, function(q) symbols_in_expr(rlang::get_expr(q)))))
  validate_property_ids(object_type, symbols)
}

validate_summary_exprs <- function(object_type, summary_quos) {
  if (length(summary_quos) == 0) {
    rlang::abort("At least one named summary expression is required.")
  }
  symbols <- unique(unlist(lapply(summary_quos, function(expr) {
    if (rlang::is_quosure(expr)) {
      symbols_in_expr(rlang::get_expr(expr))
    } else {
      symbols_in_expr(expr)
    }
  }), use.names = FALSE))
  # Strip operator/function symbols (same rationale as validate_filter_exprs)
  symbols <- setdiff(
    symbols,
    c(
      "==", "!=", "<", "<=", ">", ">=", "&", "|", "!", "+", "-", "*", "/", "^",
      "$", "[", "[[", "(", "{", "~", ":", "::", ":::",
      "c", "list", "is.na", "if_else", "case_when", "between", "in", "%in%",
      "n", "sum", "mean", "min", "max", "sd", "var", "median",
      "first", "last", "nth", "n_distinct",
      "dplyr", "base", "stats"
    )
  )
  validate_property_ids(object_type, symbols)
}

context_validate_tables <- function(ctx) {
  con <- ctx$connection
  for (obj in ctx$object_types) {
    backing <- object_backing(obj)
    # Query-backed types have no table to look up.
    if (backing$kind == "table" && !DBI::dbExistsTable(con, backing$table)) {
      rlang::abort(
        paste0("Missing backing table for object type ", obj$id, ".")
      )
    }
  }
  ctx
}

#' Check that an object type satisfies an interface
#'
#' @param object_type An \code{ontology_object_type}.
#' @param interface An \code{ontology_interface_type}.
#' @param bundle Optional \code{ontology_bundle} (as returned by
#'   \code{ontologySpecR::as_bundle()}) for link/action validation.
#' @param error Logical. If TRUE (default), abort on first violation.
#'   If FALSE, return a character vector of violation messages.
#' @return TRUE invisibly on success, or character vector of violations.
#' @keywords internal
check_implements <- function(object_type, interface, bundle = NULL, error = TRUE) {
  violations <- character(0)
  ot_id <- object_type$id
  iface_id <- interface$id

  # Object type property ids and their definitions
  ot_props_by_id <- ontologySpecR::object_properties(object_type)
  ot_prop_ids <- names(ot_props_by_id)

  # Required properties from the interface
  req_props <- interface$requiredProperties
  if (!is.null(req_props)) {
    for (req in req_props) {
      req_id <- req$id
      req_type <- req$type

      if (!(req_id %in% ot_prop_ids)) {
        violations <- c(violations, sprintf(
          "[%s] missing required property '%s' (type: %s)",
          ot_id, req_id, req_type %||% "any"
        ))
        next
      }

      # Check type match
      actual_prop <- ot_props_by_id[[req_id]]
      actual_type <- actual_prop$type
      if (!is.null(req_type) && !is.null(actual_type) && actual_type != req_type) {
        violations <- c(violations, sprintf(
          "[%s] property '%s' has type '%s' but interface requires '%s'",
          ot_id, req_id, actual_type, req_type
        ))
      }

      # Check nullable constraint
      req_nullable <- req$nullable
      actual_nullable <- actual_prop$nullable %||% TRUE
      if (identical(req_nullable, FALSE) && !identical(actual_nullable, FALSE)) {
        violations <- c(violations, sprintf(
          "[%s] property '%s' must be non-nullable per interface '%s'",
          ot_id, req_id, iface_id
        ))
      }
    }
  }

  # Check required links (only if bundle provided)
  if (!is.null(bundle)) {
    link_ids <- names(ontologySpecR::bundle_link_types(bundle))

    req_links <- interface$requiredLinks
    if (!is.null(req_links)) {
      for (req_link in req_links) {
        link_id <- req_link$linkTypeId
        if (!(link_id %in% link_ids)) {
          violations <- c(violations, sprintf(
            "[%s] missing required link type '%s'",
            ot_id, link_id
          ))
        }
      }
    }

    # Check required actions
    req_actions <- unlist(interface$requiredActions, use.names = FALSE)
    if (!is.null(req_actions)) {
      action_ids <- names(ontologySpecR::bundle_action_types(bundle))

      for (action_id in req_actions) {
        if (!(action_id %in% action_ids)) {
          violations <- c(violations, sprintf(
            "[%s] missing required action type '%s'",
            ot_id, action_id
          ))
        }
      }
    }
  }

  if (error && length(violations) > 0) {
    rlang::abort(paste(violations, collapse = "\n"))
  }

  if (length(violations) == 0) {
    invisible(TRUE)
  } else {
    violations
  }
}

#' Validate all interface implementations in a bundle
#'
#' @param bundle An \code{ontology_bundle} (as returned by
#'   \code{ontologySpecR::as_bundle()}).
#' @param error Logical. Abort on violation (TRUE) or return violations (FALSE).
#' @return Named list: object_type_id -> character vector of violations.
#'   Empty list means fully valid.
#' @keywords internal
validate_interfaces <- function(bundle, error = TRUE) {
  all_violations <- list()

  ifaces_by_id <- ontologySpecR::bundle_interface_types(bundle)

  for (obj in ontologySpecR::bundle_object_types(bundle)) {
    ot_id <- obj$id
    impls <- unlist(obj$implements, use.names = FALSE)
    if (is.null(impls)) next

    obj_violations <- character(0)
    for (iface_id in impls) {
      if (!(iface_id %in% names(ifaces_by_id))) {
        obj_violations <- c(obj_violations, sprintf(
          "[%s] implements unknown interface '%s'",
          ot_id, iface_id
        ))
        next
      }
      iface <- ifaces_by_id[[iface_id]]
      v <- check_implements(obj, iface, bundle = bundle, error = FALSE)
      if (is.character(v) && length(v) > 0) {
        obj_violations <- c(obj_violations, v)
      }
    }

    if (length(obj_violations) > 0) {
      all_violations[[ot_id]] <- obj_violations
    }
  }

  if (error && length(all_violations) > 0) {
    msg <- format_interface_violations(all_violations)
    rlang::abort(msg)
  }

  all_violations
}

#' Format interface violations for display
#'
#' @param violations Named list from validate_interfaces().
#' @return A single formatted string.
#' @keywords internal
format_interface_violations <- function(violations) {
  if (length(violations) == 0) return("")

  lines <- "Interface validation failed:"
  for (ot_id in names(violations)) {
    lines <- c(lines, sprintf("  %s:", ot_id))
    for (v in violations[[ot_id]]) {
      lines <- c(lines, sprintf("    - %s", v))
    }
  }
  paste(lines, collapse = "\n")
}

build_object_tbl <- function(ctx, object_type) {
  con <- ctx$connection
  backing <- object_backing(object_type)
  base_tbl <- if (backing$kind == "query") {
    dplyr::tbl(con, dbplyr::sql(backing$sql))
  } else {
    dplyr::tbl(con, backing$table)
  }
  props <- ontologySpecR::object_properties(object_type)
  prop_ids <- names(props)
  # Only mutate properties that need renaming or computation.
  # Skip identity mappings (column == id) to avoid self-referential
  # expressions that cause infinite recursion in dbplyr.
  exprs <- list()
  for (prop in props) {
    expression <- ontologySpecR::property_expression(prop)
    column <- ontologySpecR::property_column(prop)
    if (!is.null(expression)) {
      exprs[[prop$id]] <- dbplyr::sql(expression)
    } else if (column != prop$id) {
      exprs[[prop$id]] <- rlang::sym(column)
    }
  }
  if (length(exprs) > 0) {
    base_tbl <- do.call(dplyr::mutate, c(list(.data = base_tbl), exprs))
  }
  dplyr::select(base_tbl, dplyr::all_of(prop_ids))
}

os_new <- function(ctx, object_type_id, tbl, properties = NULL) {
  structure(
    list(
      ctx = ctx,
      object_type_id = object_type_id,
      tbl = tbl,
      properties = properties
    ),
    class = "ObjectSet"
  )
}

#' Create an ontology context
#'
#' Builds a runtime context from an \code{ontologySpecR} bundle and a DBI
#' connection. The context validates that backing tables exist for each object
#' type and optionally validates interface implementations.
#'
#' @param bundle An \code{ontologySpecR} bundle, or anything
#'   \code{ontologySpecR::as_bundle()} accepts (a list or a path to a JSON file).
#'   Legacy key spellings are mapped by \code{as_bundle()}, which warns.
#' @param connection A \code{DBI} connection.
#' @param strict Logical. If TRUE, abort on validation failures (missing tables,
#'   interface violations). If FALSE (default), warn on interface violations but
#'   still create the context. Missing tables always abort regardless of this flag.
#' @param check_interfaces Logical. If TRUE (default), validate that object types
#'   satisfy their declared interfaces. Set to FALSE to skip validation entirely.
#'
#' @return An \code{OntologyContext} object.
#' @export
ontology_context <- function(bundle, connection, strict = FALSE,
                              check_interfaces = TRUE) {
  spec <- ontologySpecR::as_bundle(bundle)
  ctx <- list(
    bundle = bundle,
    spec = spec,
    connection = connection,
    object_types = ontologySpecR::bundle_object_types(spec),
    link_types = ontologySpecR::bundle_link_types(spec),
    interface_types = ontologySpecR::bundle_interface_types(spec)
  )
  class(ctx) <- "OntologyContext"

  # Validate backing tables (always aborts on missing)
  context_validate_tables(ctx)

  # Validate interface implementations
  if (check_interfaces) {
    violations <- validate_interfaces(spec, error = FALSE)
    if (length(violations) > 0) {
      msg <- format_interface_violations(violations)
      if (strict) {
        rlang::abort(msg)
      } else {
        warning(msg, call. = FALSE)
      }
    }
  }

  ctx
}

#' @export
print.OntologyContext <- function(x, ...) {
  n_obj <- length(x$object_types)
  n_link <- length(x$link_types)
  n_iface <- length(x$interface_types)
  con_class <- class(x$connection)[[1]]
  parts <- sprintf("%d object type%s, %d link type%s",
                   n_obj, if (n_obj == 1) "" else "s",
                   n_link, if (n_link == 1) "" else "s")
  if (n_iface > 0) {
    parts <- paste0(parts, sprintf(", %d interface%s",
                                   n_iface, if (n_iface == 1) "" else "s"))
  }
  cat(sprintf("<OntologyContext> %s (%s)\n", parts, con_class))
  invisible(x)
}

#' @export
print.ObjectSet <- function(x, ...) {
  n_props <- length(x$properties)
  cat(sprintf("<ObjectSet> %s (%d propert%s) [lazy]\n",
              x$object_type_id,
              n_props,
              if (n_props == 1) "y" else "ies"))
  invisible(x)
}
