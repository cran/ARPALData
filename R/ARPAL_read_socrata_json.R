#' @keywords internal
#' @noRd

ARPAL_read_socrata_json <- function(url,
                                    where = NULL,
                                    page_size = 10000000,
                                    max_pages = Inf,
                                    api_key_id = Sys.getenv("ARPALDATA_SOCRATA_APP_TOKEN", unset = Sys.getenv("ARPALDATA_SODATA_APP_TOKEN", unset = ""))) {

  ### Modified on 2026-06-15: replace socratadata/RSocrata downloads with a
  ### minimal httr2 + jsonlite reader for the Socrata/SODA JSON API. The public
  ### ARPALData functions keep their original URL-building logic; this helper
  ### only executes the request, handles pagination, parses JSON, and returns a
  ### tibble with the raw Socrata columns expected by downstream code.

  ### Modified on 2026-06-15: set the default SODA page size to 10000000 as
  ### requested for large ARPA Lombardia downloads. If an endpoint contains more
  ### rows than page_size, the loop below still uses $offset to retrieve the
  ### remaining pages.

  ### Modified on 2026-06-15: read an optional Socrata app token from
  ### ARPALDATA_SOCRATA_APP_TOKEN, while keeping backward compatibility with
  ### ARPALDATA_SODATA_APP_TOKEN. The package works without a token because the
  ### ARPA Lombardia datasets are public; the token only helps users who perform
  ### large or frequent downloads.

  ### Normalize the URL supplied by the existing ARPALData URL-mapping functions.
  ### Older code may still generate .csv endpoints, whereas the new downloader
  ### reads the JSON API. Keeping the conversion here avoids changing the public
  ### functions and their internal URL builders.
  url <- as.character(url)[1]
  url <- stringr::str_replace(url, "\\.csv", ".json")

  ### Separate the base resource URL from an optional query string. Some internal
  ### functions already append a $where clause to the URL; this helper preserves
  ### that behavior by extracting and reusing the existing condition.
  url_split <- strsplit(url, "\\?", fixed = FALSE)[[1]]
  base_url <- url_split[1]
  query_string <- if (length(url_split) > 1) paste(url_split[-1], collapse = "?") else ""

  ### Decode the query string so that $where clauses encoded in URLs remain
  ### readable and can be passed back to httr2::req_url_query().
  query_parts <- if (nzchar(query_string)) strsplit(query_string, "&", fixed = TRUE)[[1]] else character()
  query_parts_decoded <- utils::URLdecode(query_parts)

  ### If the caller did not pass an explicit where argument, reuse the first
  ### $where/where clause found in the URL. This preserves the pre-existing
  ### package design, where filtering by sensor/date/year is often constructed
  ### upstream.
  where_from_url <- query_parts_decoded[grepl("^\\$where=|^where=", query_parts_decoded)]
  if (length(where_from_url) > 0 && is.null(where)) {
    where <- sub("^\\$?where=", "", where_from_url[1])
  }

  ### Storage for downloaded pages. Socrata returns at most $limit rows per
  ### request, so pagination is handled explicitly through $offset.
  out <- list()
  page <- 1
  offset <- 0

  repeat {

    ### Build a single SODA request. req_error(FALSE) prevents httr2 from
    ### stopping immediately on HTTP 4xx/5xx responses, allowing the function to
    ### print a package-specific error message containing the URL and response
    ### body.
    req <- httr2::request(base_url)
    req <- httr2::req_url_query(req, `$limit` = page_size, `$offset` = offset)
    req <- httr2::req_error(req, is_error = function(resp) FALSE)

    ### Add the server-side SoQL filter only when present. Empty where clauses
    ### are intentionally ignored so that metadata endpoints and unfiltered
    ### resource requests remain valid.
    if (!is.null(where) && nzchar(where)) {
      req <- httr2::req_url_query(req, `$where` = where)
    }

    ### Use the optional Socrata app token only when the user provides one. No
    ### secret or private credential is hard-coded in the package.
    if (!is.null(api_key_id) && nzchar(api_key_id)) {
      req <- httr2::req_headers(req, `X-App-Token` = api_key_id)
    }

    ### Modified on 2026-06-15: retry transient HTTP failures (for example,
    ### connection resets from the public Open Data Lombardia endpoint) before
    ### failing the request. These errors are not deterministic and may occur
    ### during examples or CRAN checks when public endpoints are under load.
    resp <- NULL
    req_error <- NULL
    for (req_attempt in seq_len(3)) {
      resp <- tryCatch(
        httr2::req_perform(req),
        error = function(e) {
          req_error <<- e
          NULL
        }
      )
      if (!is.null(resp)) {
        break
      }
      if (req_attempt < 3) {
        Sys.sleep(2 * req_attempt)
      }
    }
    if (is.null(resp)) {
      stop(
        paste0(
          "ARPA Lombardia / Socrata API request failed after 3 attempts.\n\nURL:\n",
          req$url, "\n\nError:\n", conditionMessage(req_error)
        ),
        call. = FALSE
      )
    }

    ### Read the HTTP status and response body. The body is kept as text until
    ### after status checks so that API error messages can be reported verbatim.
    status <- httr2::resp_status(resp)
    body <- httr2::resp_body_string(resp)

    ### Modified on 2026-06-15: fall back to an unauthenticated public request
    ### if the optional app token is invalid. This makes user-side token mistakes
    ### non-fatal for public data downloads.
    if (status == 403 && grepl("Invalid app_token", body, fixed = TRUE)) {
      req <- httr2::request(base_url)
      req <- httr2::req_url_query(req, `$limit` = page_size, `$offset` = offset)
      req <- httr2::req_error(req, is_error = function(resp) FALSE)

      if (!is.null(where) && nzchar(where)) {
        req <- httr2::req_url_query(req, `$where` = where)
      }

      ### Retry the unauthenticated fallback request as well, because transient
      ### connection resets may also occur here.
      resp <- NULL
      req_error <- NULL
      for (req_attempt in seq_len(3)) {
        resp <- tryCatch(
          httr2::req_perform(req),
          error = function(e) {
            req_error <<- e
            NULL
          }
        )
        if (!is.null(resp)) {
          break
        }
        if (req_attempt < 3) {
          Sys.sleep(2 * req_attempt)
        }
      }
      if (is.null(resp)) {
        stop(
          paste0(
            "ARPA Lombardia / Socrata API request failed after 3 attempts.\n\nURL:\n",
            req$url, "\n\nError:\n", conditionMessage(req_error)
          ),
          call. = FALSE
        )
      }
      status <- httr2::resp_status(resp)
      body <- httr2::resp_body_string(resp)
    }

    ### Stop on HTTP errors after the invalid-token fallback has been attempted.
    ### Including the full URL and body is useful for debugging changed Socrata
    ### field names, invalid SoQL clauses, or retired datasets.
    if (status >= 400) {
      stop(
        paste0(
          "ARPA Lombardia / Socrata API error. HTTP status: ", status,
          "\n\nURL:\n", req$url,
          "\n\nBody:\n", body
        ),
        call. = FALSE
      )
    }

    ### Parse the JSON page. jsonlite::fromJSON(flatten = TRUE) returns a data
    ### frame for ordinary Socrata row arrays and an empty object when the query
    ### returns no rows.
    tmp <- jsonlite::fromJSON(body, flatten = TRUE)

    ### No rows in the current page means that the download is complete. This
    ### also covers genuinely empty query results.
    if (length(tmp) == 0 || nrow(tmp) == 0) {
      break
    }

    ### Store the page as a tibble while preserving raw Socrata column names.
    ### Downstream ARPALData functions perform all semantic renaming and type
    ### conversion.
    out[[page]] <- tibble::as_tibble(tmp)

    ### If fewer rows than page_size are returned, this is the last page.
    if (nrow(tmp) < page_size) {
      break
    }

    ### max_pages is primarily a safety/debugging argument. In normal package
    ### use it remains Inf.
    if (page >= max_pages) {
      break
    }

    ### Move to the next SODA page.
    offset <- offset + page_size
    page <- page + 1
  }

  ### Modified on 2026-06-15: return an empty tibble with the raw Socrata
  ### columns expected by downstream ARPALData processing functions when an
  ### endpoint/query returns no rows. This prevents empty API responses from
  ### dropping required columns such as idsensore, data, valore, and stato.
  if (length(out) == 0) {
    return(tibble::tibble(
      idsensore = character(),
      data = character(),
      valore = character(),
      stato = character(),
      operatore = character(),
      ### Modified on 2026-06-15: include idoperatore for municipal AQ endpoints,
      ### which use this raw column in downstream processing.
      idoperatore = character()
    ))
  }

  ### Combine all downloaded pages into a single tibble.
  dplyr::bind_rows(out)
}
