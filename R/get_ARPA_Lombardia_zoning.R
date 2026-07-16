#' Download ARPA Lombardia zoning geometries
#'
#' @description 'get_ARPA_Lombardia_zoning' returns the geometries (polygonal shape file) and a map of
#' the ARPA zoning of Lombardy. The zoning reflects the main orographic characteristics of the territory.
#' Lombardy region is classified into seven type of areas: large urbanized areas, urbanized areas in rural
#' contexts, rural areas, mountainous areas and valley bottom.
#' For more information, visit the section 'Zonizzazione ARPA Lombardia' at the websites
#' https://www.arpalombardia.it/temi-ambientali/aria/rete-di-rilevamento/classificazione-zone/ and
#' https://www.arpalombardia.it/temi-ambientali/aria/mappa-della-zonizzazione/
#'
#' @param plot_map Logical value (FALSE or TRUE). If plot_map = TRUE, the ARPA Lombardia zoning is represented
#' on a map, if plot_mat = FALSE only the geometry (polygon shapefile) is stored in the output.
#' Default is plot_map = TRUE.
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
#' zones <- get_ARPA_Lombardia_zoning(plot_map = TRUE)
#'
#' @export

get_ARPA_Lombardia_zoning <-
  function(plot_map = TRUE, title = "ARPA Lombardia zoning", line_type = 1,
           line_size = 1, xlab = "Longitude", ylab = "Latitude") {

    ##### Define %notin%
    '%notin%' <- Negate('%in%')

    ##### Checks if by_sensor setup properly
    if (plot_map %notin% c(FALSE,TRUE)) {
      stop("Wrong setup for 'plot_map'. Use 1 or 0 or TRUE or FALSE.",
           call. = FALSE)
    }

    ##### Read zoning shapefile bundled with the package
    ### Modified on 2026-06-26: use the package-bundled ARPA zoning shapefile
    ### stored in inst/extdata, instead of checking and downloading the same
    ### zip archive from the ARPALData GitHub repository at runtime.
    temp1 <- ARPAL_extdata_path("ARPA_zoning_shape.zip")
    temp2 <- tempfile()
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
        ggplot2::theme_bw() +
        ggplot2::theme(legend.position="bottom") +
        ggplot2::guides(fill=guide_legend(nrow=2,byrow=TRUE)) +
        ggplot2::scale_x_continuous(labels = function(x) paste0(x, '\u00B0', "E")) +
        ggplot2::scale_y_continuous(labels = function(x) paste0(x, '\u00B0', "N"))
      print(geo_plot)
    }

    file.remove(your_SHP_file)

    attr(Zoning, "class") <- c("ARPALdf","tbl_df","tbl","data.frame","sf")

    return(Zoning)
  }
