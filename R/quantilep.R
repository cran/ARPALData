#' @keywords internal
#' @noRd

quantilep <- function(x, perc_str) {
  p <- as.numeric(sub(".*q", "",perc_str))/100
  p_names <- purrr::map_chr(p, ~ paste0("q",.x*100))
  p_funs <- purrr::map(p, ~ purrr::partial(stats::quantile, probs = .x, na.rm = TRUE)) %>%
    purrr::set_names(nm = p_names)
  return(p_funs)
}

# quantilep <- function(x,perc_str) {
#   p <- as.numeric(sub(".*q", "",perc_str))/100
#   # stats::quantile(x = x, probs = p, na.rm = T)
#   p_funs <- map(p, ~partial(quantile, probs = .x, na.rm = TRUE))
# }

# quantilep <- function(x, perc_str) {
#   p <- as.numeric(sub(".*q", "",perc_str))/100
#   do.call(cbind, lapply(x,stats::quantile(x = x, probs = p, na.rm = T)))
# }

# quantilep <- function(x, perc_str) {
#   p <- as.numeric(sub(".*q", "",perc_str))/100
#   # tibble::tibble(x = stats::quantile(x = x, probs = p, na.rm = T))
#   # do.call(cbind, lapply(x,quantilep(x,fns_vec)))
#   stats::quantile(x = x, probs = p, na.rm = T)
#   # return(y)
# }

# quibble2 <- function(x, q = c(0.25, 0.5, 0.75)) {
#   tibble("{{ x }}" := quantile(x, q), "{{ x }}_q" := q)
# }


