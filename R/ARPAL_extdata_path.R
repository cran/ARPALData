#' @keywords internal
#' @noRd

ARPAL_extdata_path <- function(filename) {
  ### Modified on 2026-06-26: retrieve package-bundled support files from
  ### inst/extdata instead of downloading them from the ARPALData GitHub
  ### repository at runtime. This keeps mapping and metadata helpers available
  ### offline once the package is installed.
  path <- system.file("extdata", filename, package = "ARPALData")

  if (!nzchar(path) || !file.exists(path)) {
    stop(
      "Required package file '", filename, "' was not found in inst/extdata. ",
      "Please reinstall ARPALData or report a bug.",
      call. = FALSE
    )
  }

  return(path)
}
