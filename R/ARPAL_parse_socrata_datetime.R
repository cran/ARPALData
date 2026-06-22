#' @keywords internal
#' @noRd

ARPAL_parse_socrata_datetime <- function(x) {

  ### Modified on 2026-06-15: parse Socrata JSON date/datetime strings
  ### robustly and return POSIXct values. This helper is used in functions
  ### that expose sub-daily observations, where preserving the hour/minute
  ### component is required.

  ### Convert the input to character to make the parser robust to the classes
  ### returned by jsonlite::fromJSON(), readr, or intermediate package code.
  x <- as.character(x)

  ### Socrata JSON endpoints commonly return timestamps in ISO-like form, such
  ### as "2026-03-01T12:30:00.000". lubridate::ymd_hms() handles these strings
  ### and returns POSIXct values. Warnings are suppressed because a fallback is
  ### intentionally applied to date-only strings.
  out <- suppressWarnings(lubridate::ymd_hms(x, tz = "UTC", quiet = TRUE))

  ### Values not parsed by ymd_hms() but still present are generally date-only
  ### strings. They are interpreted as midnight UTC of the corresponding date.
  idx <- is.na(out) & !is.na(x) & nzchar(x)

  if (any(idx)) {
    out[idx] <- suppressWarnings(
      as.POSIXct(lubridate::ymd(x[idx], quiet = TRUE), tz = "UTC")
    )
  }

  ### Return POSIXct values. This keeps hourly and 10-minute outputs as proper
  ### date-time columns instead of character vectors.
  out
}
