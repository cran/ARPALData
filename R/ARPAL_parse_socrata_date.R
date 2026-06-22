#' @keywords internal
#' @noRd

ARPAL_parse_socrata_date <- function(x) {

  ### Modified on 2026-06-15: parse Socrata JSON date/datetime strings
  ### robustly and return a Date vector. Open Data Lombardia/Socrata
  ### endpoints may encode daily fields either as simple dates
  ### (for example, "2026-03-01") or as full timestamps
  ### (for example, "2026-03-01T00:00:00.000"). Using lubridate::ymd()
  ### alone would turn full timestamps into NA in some code paths.

  ### Convert the input to character first. This keeps the helper stable when
  ### the API parser returns character, Date, POSIXct, numeric-looking strings,
  ### or empty vectors.
  x <- as.character(x)

  ### First try the most informative parser, lubridate::ymd_hms(). This handles
  ### the standard Socrata datetime representation returned by JSON endpoints.
  ### Warnings are suppressed because failures are expected for pure YYYY-MM-DD
  ### strings and are handled in the fallback step below.
  out <- suppressWarnings(lubridate::ymd_hms(x, tz = "UTC", quiet = TRUE))

  ### Identify values that were not parsed as datetimes but are still non-empty
  ### input strings. These are usually pure dates and can be safely parsed with
  ### lubridate::ymd().
  idx <- is.na(out) & !is.na(x) & nzchar(x)

  ### Fallback parser for pure dates. The result is temporarily converted to
  ### POSIXct so that it can be assigned into the POSIXct vector created above.
  if (any(idx)) {
    out[idx] <- suppressWarnings(
      as.POSIXct(lubridate::ymd(x[idx], quiet = TRUE), tz = "UTC")
    )
  }

  ### Downstream daily/municipal metadata routines expect Date objects, not
  ### POSIXct objects. The final coercion also strips any irrelevant midnight
  ### time component returned by Socrata.
  as.Date(out)
}
