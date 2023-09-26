#' Generate a map of the selected stations
#'
#' @description 'get_ARPA_Lombardia_AQ_data' represents on a map (geometries/polygon of Lombardy) the location of
#' the stations contained in a data frame of class 'ARPALdf'. Data can be either a ARPALdf of observed data
#' (from 'get_ARPA_Lombardia_xxx' commands) and an ARPALdf obtained as registry
#' (from 'get_ARPA_Lombardia_xxx_registry' command).
#'
#' @param data Dataset of class 'ARPALdf' containing the stations to plot on the map. It can be either a
#' ARPALdf of observed data (from 'get_ARPA_Lombardia_xxx' commands) and an ARPALdf obtained as registry
#' (from 'get_ARPA_Lombardia_xxx_registry' command).
#' @param title Title of the plot. Deafult is 'Map of ARPA stations in Lombardy'
#' @param prov_line_type Linetype for Lombardy provinces. Default is 1.
#' @param prov_line_size Size of the line for Lombardy provinces. Default is 1.
#' @param col_points Color of the points. Default is 'blue'.
#' @param xlab x-axis label. Default is 'Longitude'.
#' @param ylab y-axis label. Default is 'Latitude'.
#'
#' @return A map of selected stations across the Lombardy region
#'
#' @examples
#' \donttest{
#' ## Download daily concentrations observed at all the stations in 2020.
#' if (require("RSocrata")) {
#'     d <- get_ARPA_Lombardia_AQ_data(ID_station = NULL, Date_begin = "2020-01-01",
#'             Date_end = "2020-12-31", Frequency = "daily")
#' }
#' ## Map the stations included in 'd'
#' map_Lombardia_stations(data = d, title = "Air quality stations in Lombardy")
#' }
#'
#' @export

map_Lombardia_stations <-
  function(data, title = "Map of ARPA stations in Lombardy", prov_line_type = 1,
           prov_line_size = 1, col_points = "blue",
           xlab = "Longitude", ylab = "Latitude") {

    Lombardia <- get_Lombardia_geospatial(NUTS_level = "NUTS3")

    if(is_ARPALdf_AQ(Data = data) == T) {
      Stats <- get_ARPA_Lombardia_AQ_registry()
    } else if (is_ARPALdf_W(Data = data) == T) {
      Stats <- get_ARPA_Lombardia_W_registry()
    }

    data <- data.frame(IDStation = unique(data$IDStation))
    d <- dplyr::left_join(data,Stats,by="IDStation")
    d <- d %>%
      sf::st_as_sf(coords = c("Longitude", "Latitude"),crs = 4326)

    geo_plot <- Lombardia %>%
      ggplot2::ggplot() +
      ggplot2::geom_sf(linetype = prov_line_type, size = prov_line_size) +
      ggplot2::geom_sf(data = d, col=col_points) +
      ggplot2::labs(title = title) +
      ggplot2::theme_bw() +
      ggplot2::scale_x_continuous(labels = function(x) paste0(x, '\u00B0', "E")) +
      ggplot2::scale_y_continuous(labels = function(x) paste0(x, '\u00B0', "N"))

    print(geo_plot)
  }


