`%||%` <- function(x, y) {
  if (is.null(x)) {
    y
  } else {
    x
  }
}

ensure_object_set <- function(os) {
  if (!inherits(os, "ObjectSet")) {
    rlang::abort("Expected an ObjectSet.")
  }
}

ensure_same_object_type <- function(os1, os2) {
  if (os1$object_type_id != os2$object_type_id) {
    rlang::abort("Object sets must have the same object type.")
  }
}

resolve_target_columns <- function(joined_tbl, target_props, suffix = ".to") {
  # Ensure we have character property ids
  if (!is.character(target_props)) {
    rlang::abort("`target_props` must be a character vector of property ids.")
  }
  
  vars <- dplyr::tbl_vars(joined_tbl)
  
  source_names <- vapply(
    target_props,
    function(prop) {
      prop_to <- paste0(prop, suffix)
      if (prop_to %in% vars) {
        prop_to
      } else if (prop %in% vars) {
        prop
      } else {
        NA_character_
      }
    },
    character(1)
  )
  
  if (anyNA(source_names)) {
    missing <- target_props[is.na(source_names)]
    rlang::abort(paste0(
      "Missing target properties after join: ",
      paste(missing, collapse = ", ")
    ))
  }
  
  rlang::set_names(rlang::syms(source_names), target_props)
}

# One string key per row. A single column becomes as.character(); several
# columns are joined with "|". Done column by column: going through
# as.matrix()/apply() would pad numbers to a common width ("1" vs "10").
compose_node_key <- function(values) {
  if (!is.list(values)) {
    return(as.character(values))
  }
  do.call(paste, c(lapply(unname(as.list(values)), as.character), list(sep = "|")))
}

# Graph node keys are namespaced by object type, so Airport 1 and FlightRoute 1
# are distinct nodes.
graph_node_key <- function(object_type_id, pk_values) {
  paste0(object_type_id, "::", compose_node_key(pk_values))
}

# vertexR is optional (Suggests). Resolve one of its exported functions, failing
# with a clear message if the package is missing or too old to provide it,
# rather than a bare "not an exported object" from `::`.
vertexr_fn <- function(name, pkg = "vertexR") {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    rlang::abort(sprintf("os_traverse_deep() requires the %s package.", pkg))
  }
  tryCatch(
    getExportedValue(pkg, name),
    error = function(e) {
      rlang::abort(sprintf(
        "The installed %s does not provide `%s()`; update it to a version with the DuckPGQ backend.",
        pkg, name
      ))
    }
  )
}

is_duckdb_connection <- function(con) {
  inherits(con, "duckdb_connection")
}
