#' @export
print.ObjectSet <- function(x, ...) {
  object_type <- x$object_type_id %||% "<unknown>"
  props <- x$properties %||% character(0)
  props_label <- if (length(props) == 1) "property" else "properties"
  message(
    sprintf(
      "<ObjectSet> %s (%d %s) [lazy]",
      object_type,
      length(props),
      props_label
    )
  )
  invisible(x)
}

#' @export
print.OntologyContext <- function(x, ...) {
  object_count <- length(x$object_types %||% list())
  link_count <- length(x$link_types %||% list())
  message(
    sprintf(
      "<OntologyContext> %d object types, %d link types",
      object_count,
      link_count
    )
  )
  invisible(x)
}
