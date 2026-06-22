#' @keywords internal
#' @noRd

quantilep <- function(x, perc_str) {
  ### Modified on 2026-06-15: return an empty named list when no quantile
  ### functions are requested. This preserves the previous behavior inside
  ### dynamic dplyr summaries and avoids assigning one name to a zero-length
  ### list when Fns_vec does not contain strings such as "q25" or "q75".
  if (length(perc_str) == 0) {
    return(stats::setNames(list(), character(0)))
  }

  p <- suppressWarnings(as.numeric(sub(".*q", "", perc_str)) / 100)
  p <- p[is.finite(p)]

  if (length(p) == 0) {
    return(stats::setNames(list(), character(0)))
  }

  ### Modified on 2026-06-15: replace purrr::map_chr(), purrr::map(),
  ### purrr::partial() and purrr::set_names() with base R equivalents, so
  ### the package does not need to import purrr only for this small helper.
  p_names <- paste0("q", p * 100)
  p_funs <- lapply(p, function(pp) {
    force(pp)
    function(x, ...) {
      stats::quantile(x, probs = pp, na.rm = TRUE, ...)
    }
  })
  names(p_funs) <- p_names

  return(p_funs)
}



