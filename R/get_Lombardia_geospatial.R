#' Download geospatial data of Lombardy from Eurostat
#'
#' @description 'get_Lombardia_geospatial' returns the polygonal (shape file) object containing the geometries
#' of Lombardy. Shapefile are available at different NUTS levels: 'LAU' for the shapefile of municipalities of
#' Lombardy, 'NUTS3' for the shapefile of provinces of Lombardy and 'NUTS2' for the shapefile of Lombardy.
#'
#' @param NUTS_level NUTS level required: use "NUTS2" for regional geometries, "NUTS3" for provincial geometries,
#' or "LAU" for municipal geometries. Default NUTS_level = "LAU".
#'
#' @return A data frame of class 'data.frame', "sf" and 'ARPALdf'.
#'
#' @examples
#' # shape <- get_Lombardia_geospatial(NUTS_level = "LAU")
#'
#' @export

get_Lombardia_geospatial <- function(NUTS_level = "LAU") {

  '%notin%' <- Negate('%in%')

  ### Checks
  if (NUTS_level %notin% c("NUTS2","NUTS3","LAU")) {
    stop("Selected NUTS not available: use one of 'NUTS2', 'NUTS3' and 'LAU'",call. = FALSE)
  }

  # Dowload shape file for Lombardy municipalities
  temp1 <- tempfile()
  temp2 <- tempfile()
  download.file(url = "https://github.com/PaoloMaranzano/ARPALData/raw/main/Shape_Comuni_Lombardia.zip",
                destfile = temp1)
  unzip(zipfile = temp1, exdir = temp2)
  your_SHP_file<-list.files(temp2, pattern = ".shp$",full.names=TRUE)

  # Read the shapefile
  Lombardia <- sf::read_sf(your_SHP_file) %>%
    sf::st_as_sf(crs = 4326) %>%
    dplyr::rename(Prov_name = .data$NOME_PRO,City = .data$NOME_COM, City_code_ISTAT = .data$ISTAT) %>%
    dplyr::mutate(Prov_name = case_when(.data$Prov_name == "VARESE" ~ "Varese",
                                        .data$Prov_name == "COMO" ~ "Como",
                                        .data$Prov_name == "LECCO" ~ "Lecco",
                                        .data$Prov_name == "SONDRIO" ~ "Sondrio",
                                        .data$Prov_name == "BERGAMO" ~ "Bergamo",
                                        .data$Prov_name == "BRESCIA" ~ "Brescia",
                                        .data$Prov_name == "PAVIA" ~ "Pavia",
                                        .data$Prov_name == "LODI" ~ "Lodi",
                                        .data$Prov_name == "CREMONA" ~ "Cremona",
                                        .data$Prov_name == "MANTOVA" ~ "Mantova",
                                        .data$Prov_name == "MILANO" ~ "Milano",
                                        .data$Prov_name == "MONZA E DELLA BRIANZA" ~ "Monza e della Brianza")) %>%
    dplyr::select(.data$Prov_name, .data$City, .data$City_code_ISTAT, .data$geometry) %>%
    dplyr::mutate(City = stringi::stri_trans_general(str = .data$City, id = "Latin-ASCII"),
                  City = toupper(.data$City),
                  City = gsub("\\-", " ", .data$City),
                  City = tm::removePunctuation(.data$City),
                  City = tm::removeNumbers(.data$City),
                  City = tm::stripWhitespace(.data$City),
                  City = stringr::str_to_title(.data$City))

  # Associating NUTS codes (from Eurostat) to each observation
  Eurostat <- eurostat::get_eurostat_geospatial(output_class = "sf",resolution = 60, nuts_level = 3, year = 2016)
  Eurostat <- Eurostat %>%
    sf::st_as_sf(crs = 4326) %>%
    data.frame() %>%
    dplyr::filter(grepl("ITC4",.data$geo)) %>%
    dplyr::select(Prov_name = .data$NUTS_NAME, Prov_code_EUROSTAT = .data$NUTS_ID) %>%
    dplyr::mutate(Reg_name = "Lombardia", Reg_code_EUROSTAT = "ITC4")

  # Joining ISTAT metadata and Eurostat metadata
  Lombardia <- dplyr::left_join(Lombardia,Eurostat,by=c("Prov_name"))
  Lombardia <- Lombardia %>%
    dplyr::select(.data$Reg_name,.data$Reg_code_EUROSTAT,.data$Prov_name,.data$Prov_code_EUROSTAT,
                  .data$City,.data$City_code_ISTAT,.data$geometry)

  if (NUTS_level == "NUTS3") {
    Lombardia <- Lombardia %>%
      dplyr::group_by(.data$Reg_name,.data$Reg_code_EUROSTAT,.data$Prov_name,.data$Prov_code_EUROSTAT) %>%
      dplyr::summarise(.groups = "keep") %>%
      dplyr::ungroup()
  }

  if (NUTS_level == "NUTS2") {
    Lombardia <- Lombardia %>%
      dplyr::group_by(.data$Reg_name,.data$Reg_code_EUROSTAT) %>%
      dplyr::summarise(.groups = "keep") %>%
      dplyr::ungroup()
  }

  file.remove(your_SHP_file)

  attr(Lombardia, "class") <- c("ARPALdf","ARPALdf_AQ","tbl_df","tbl","data.frame","sf")

  return(Lombardia)
}

