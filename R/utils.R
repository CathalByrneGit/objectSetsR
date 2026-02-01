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
  vars <- dplyr::tbl_vars(joined_tbl)
  source_names <- vapply(target_props, function(prop) {
    if (paste0(prop, suffix) %in% vars) {
      paste0(prop, suffix)
    } else if (prop %in% vars) {
      prop
    } else {
      NA_character_
    }
  }, character(1))
  if (anyNA(source_names)) {
    missing <- target_props[is.na(source_names)]
    rlang::abort(
      paste0(
        "Missing target properties after join: ",
        paste(missing, collapse = ", ")
      )
    )
  }
  rlang::set_names(rlang::syms(source_names), target_props)
}

compose_node_key <- function(values) {
  if (is.null(dim(values))) {
    as.character(values)
  } else {
    apply(values, 1, function(row) paste(row, collapse = "|"))
  }
}
