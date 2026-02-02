bundle_as_list <- function(bundle) {
  if (inherits(bundle, "ontology_bundle")) {
    return(ontologySpecR::as_list(bundle))
  }
  if (is.list(bundle)) {
    return(bundle)
  }
  rlang::abort("`bundle` must be a list or ontologySpecR bundle.")
}

index_by_id <- function(items) {
  stats::setNames(items, vapply(items, `[[`, character(1), "id"))
}

get_object_types <- function(bundle_list) {
  obj_types <- bundle_list$objects %||% bundle_list$objectTypes %||%
    bundle_list$object_types
  if (is.null(obj_types)) {
    rlang::abort("Bundle is missing object types.")
  }
  index_by_id(obj_types)
}

get_link_types <- function(bundle_list) {
  link_types <- bundle_list$links %||% bundle_list$linkTypes %||%
    bundle_list$link_types
  if (is.null(link_types)) {
    rlang::abort("Bundle is missing link types.")
  }
  index_by_id(link_types)
}

get_interface_types <- function(bundle_list) {
  ifaces <- bundle_list$interfaces %||% bundle_list$interfaceTypes %||%
    bundle_list$interface_types
  if (is.null(ifaces)) return(list())
  index_by_id(ifaces)
}

get_interface <- function(ctx, interface_id) {
  iface <- ctx$interface_types[[interface_id]]
  if (is.null(iface)) {
    rlang::abort(paste0("Unknown interface: ", interface_id))
  }
  iface
}

interface_property_ids <- function(interface_type) {
  props <- interface_type$properties %||% interface_type$propertys %||%
    interface_type$props
  if (is.null(props) || length(props) == 0) {
    rlang::abort(paste0("Interface ", interface_type$id, " has no properties."))
  }
  vapply(props, `[[`, character(1), "id")
}

find_implementing_types <- function(ctx, interface_id) {
  matches <- character(0)
  for (obj in ctx$object_types) {
    impls <- obj$implements %||% obj$interfaces
    if (is.null(impls)) next
    if (is.list(impls)) impls <- unlist(impls, use.names = FALSE)
    if (interface_id %in% impls) {
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

object_properties <- function(object_type) {
  props <- object_type$properties %||% object_type$propertys %||% object_type$props
  if (is.null(props)) {
    rlang::abort(paste0("Object type ", object_type$id, " has no properties."))
  }
  props
}

object_primary_key <- function(object_type) {
  pk <- object_type$primaryKey %||% object_type$primary_key %||% object_type$primary_key_id
  if (is.null(pk)) {
    rlang::abort(paste0("Object type ", object_type$id, " has no primary key."))
  }
  # ontologySpecR produces list(properties = list("col"), strategy = "natural")
  if (is.list(pk) && !is.null(pk$properties)) {
    return(as.character(pk$properties))
  }
  # plain string or character vector
  as.character(pk)
}

object_source_table <- function(object_type) {
  source <- object_type$source
  if (is.null(source)) {
    rlang::abort(paste0("Object type ", object_type$id, " has no source binding."))
  }
  table <- source$table %||% source$name %||% source$id %||% object_type$id
  schema <- source$schema
  if (!is.null(schema)) {
    return(DBI::Id(schema = schema, table = table))
  }
  table
}

property_source_expr <- function(property) {
  source <- property$source %||% list()
  column <- source$column %||% source$name %||% property$id
  expression <- source$expression
  list(column = column, expression = expression)
}

property_ids <- function(object_type) {
  vapply(object_properties(object_type), `[[`, character(1), "id")
}

validate_property_ids <- function(object_type, symbols) {
  props <- property_ids(object_type)
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
    table <- object_source_table(obj)
    if (!DBI::dbExistsTable(con, table)) {
      rlang::abort(
        paste0("Missing backing table for object type ", obj$id, ".")
      )
    }
  }
  ctx
}

build_object_tbl <- function(ctx, object_type) {
  con <- ctx$connection
  table <- object_source_table(object_type)
  base_tbl <- dplyr::tbl(con, table)
  props <- object_properties(object_type)
  prop_ids <- vapply(props, `[[`, character(1), "id")
  # Only mutate properties that need renaming or computation.
  # Skip identity mappings (column == id) to avoid self-referential
  # expressions that cause infinite recursion in dbplyr.
  exprs <- list()
  for (prop in props) {
    mapping <- property_source_expr(prop)
    if (!is.null(mapping$expression)) {
      exprs[[prop$id]] <- dbplyr::sql(mapping$expression)
    } else if (mapping$column != prop$id) {
      exprs[[prop$id]] <- rlang::sym(mapping$column)
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
#' type and is required for creating lazy \code{ObjectSet} instances.
#'
#' @param bundle A bundle object from \code{ontologySpecR} or a compatible list.
#' @param connection A \code{DBI} connection.
#'
#' @return An \code{OntologyContext} object.
#' @export
ontology_context <- function(bundle, connection) {
  bundle_list <- bundle_as_list(bundle)
  ctx <- list(
    bundle = bundle,
    bundle_list = bundle_list,
    connection = connection,
    object_types = get_object_types(bundle_list),
    link_types = get_link_types(bundle_list),
    interface_types = get_interface_types(bundle_list)
  )
  class(ctx) <- "OntologyContext"
  context_validate_tables(ctx)
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
