#' Download ARPA Lombardia zoning geometries
#'
#' @description 'get_ARPA_Lombardia_zoning' returns the geometries (polygonal shape file) and a map of
#' the ARPA zoning of Lombardy. The zoning reflects the main orographic characteristics of the territory.
#' Lombardy region is classified into seven type of areas: large urbanised areas, urbanized areas in rural
#' contexts, rural areas, mountainous areas and valley bottom.
#' For more information about the municipal data visit the section 'Zonizzazione ARPA Lombardia' at the webpage:
#' https://www.arpalombardia.it/Pages/Aria/Rete-di-rilevamento/Zonizzazione.aspx
#'
#' @param plot_map Logic value (0 or 1). If plot_map = 1, the ARPA Lombardia zoning is represented
#' on a map, if plot_mat = 0 only the geometry (polygon shapefile) is stored in the output.
#' Default is plot_map = 1.
#' @param title Title of the plot. Deafult is 'ARPA Lombardia zoning'
#' @param line_type Linetype for the zones' borders. Default is 1.
#' @param line_size Size of the line for the zones. Default is 1.
#' @param xlab x-axis label. Default is 'Longitude'.
#' @param ylab y-axis label. Default is 'Latitude'.
#'
#' @return The function returns an object of class 'sf' containing the polygon borders of the seven zones used by
#' ARPA Lombardia to classify the regional territory. If plot_map = 1, it also returns a map of the zoning.
#'
#' @examples
#' zones <- get_ARPA_Lombardia_zoning(plot_map = 1)
#'
#' @export

get_ARPA_Lombardia_zoning <-
  function(plot_map = 1, title = "ARPA Lombardia zoning", line_type = 1,
           line_size = 1, xlab = "Longitude", ylab = "Latitude") {

    # Dowload shape file for Lombardy municipalities
    temp1 <- tempfile()
    temp2 <- tempfile()
    download.file(url = "https://github.com/PaoloMaranzano/ARPALData/raw/main/ARPA_zoning_shape.zip",
                  destfile = temp1)
    unzip(zipfile = temp1, exdir = temp2)
    your_SHP_file <- list.files(temp2, pattern = ".shp$",full.names=TRUE)

    # Read and reshape the shapefile
    Zoning <- sf::read_sf(your_SHP_file) %>%
      sf::st_as_sf(crs = 4326) %>%
      dplyr::mutate(Zone = case_when(.data$COD_ZONA == "A" ~ "Urbanized Plain",
                                     .data$COD_ZONA == "Agg_BG" ~ "Metropolitan area of Bergamo",
                                     .data$COD_ZONA == "Agg_BS" ~ "Metropolitan area of Brescia",
                                     .data$COD_ZONA == "Agg_MI" ~ "Metropolitan area of Milano",
                                     .data$COD_ZONA == "B" ~ "Rural Plain",
                                     .data$COD_ZONA == "C" ~ "Mountain",
                                     .data$COD_ZONA == "D" ~ "Valley floor")) %>%
      dplyr::select(Cod_Zone = .data$COD_ZONA, .data$Zone, .data$geometry)

    if (plot_map == 1) {
      geo_plot <- Zoning %>%
        ggplot2::ggplot() +
        ggplot2::geom_sf(aes(fill = .data$Zone),linetype = line_type, size = line_size) +
        ggplot2::labs(title = title, x = xlab, y = ylab) +
        ggplot2::theme(legend.position="bottom") +
        ggplot2::guides(fill=guide_legend(nrow=2,byrow=TRUE))
      print(geo_plot)
    }

    file.remove(your_SHP_file)

    attr(Zoning, "class") <- c("ARPALdf","tbl_df","tbl","data.frame","sf")

    return(Zoning)
  }
