#' @keywords internal
#' @noRd

get_ARPA_Lombardia_W_data_1y <-
  function(ID_station = NULL, Year = 2019, Var_vec = NULL, by_sensor = 0, verbose = T) {

    Metadata <- W_metadata_reshape()
    Metadata <- Metadata %>%
      dplyr::select(-c(.data$Altitude,.data$Province,
                       .data$DateStart,.data$DateStop,
                       .data$Latitude,.data$Longitude))
    if (!is.null(ID_station)) {
      Metadata <- Metadata %>%
        dplyr::filter(.data$IDStation %in% ID_station)
    }
    if (!is.null(Var_vec)) {
      Metadata <- Metadata %>%
        dplyr::filter(.data$Pollutant %in% Var_vec)
    }

    url <- url_dataset_year(Stat_type = "W", Year = Year)

    if (verbose==T) {
      cat("Downloading data from ARPA Lombardia: started at", as.character(Sys.time()), "\n")
    }
    zip_file <- tempfile(fileext = ".zip")
    download.file(url = url, destfile = zip_file, mode = "wb")
    if (verbose==T) {
      cat("Importing data: started at", as.character(Sys.time()), "\n")
    }
    Meteo <- tibble::tibble(data.table::fread(unzip(zip_file, files = paste0(Year,".csv"))))
    if (verbose==T) {
      cat("Processing data: started at", as.character(Sys.time()), "\n")
    }
    Meteo <- Meteo %>%
      dplyr::select(IDSensor = .data$IdSensore, Date = .data$Data, Value = .data$Valore,
                    Operator = .data$idOperatore) %>%
      dplyr::mutate(Date = lubridate::dmy_hms(.data$Date))

    file.remove(paste0(Year,".csv"))

    Meteo <- dplyr::right_join(Meteo,Metadata, by = "IDSensor")

    if (by_sensor %in% c(1,TRUE)) {
      Meteo <- Meteo %>%
        dplyr::filter(!is.na(.data$Date)) %>%
        dplyr::mutate(Operator = dplyr::case_when(.data$Measure == "Relative_humidity" & .data$Operator == 3 ~ 1,
                                                  .data$Measure == "Relative_humidity" & .data$Operator == 2 ~ 1,
                                                  .data$Measure == "Temperature" & .data$Operator == 3 ~ 1,
                                                  .data$Measure == "Temperature" & .data$Operator == 2 ~ 1,
                                                  TRUE ~ as.numeric(.data$Operator)),
                      Measure = dplyr::case_when(.data$Measure == "Wind_direction" & .data$Operator == 3 ~ "Wind_direction_max",
                                                 .data$Measure == "Wind_speed" & .data$Operator == 3 ~ "Wind_speed_max",
                                                 TRUE ~ as.character(.data$Measure))) %>%
        dplyr::select(-c(.data$Operator)) %>%
        dplyr::select(.data$Date,.data$IDStation,.data$NameStation,.data$IDSensor,
                      .data$Measure,.data$Value)
    } else if (by_sensor %in% c(0,FALSE)) {
      Meteo <- Meteo %>%
        dplyr::filter(!is.na(.data$Date)) %>%
        dplyr::mutate(Operator = dplyr::case_when(.data$Measure == "Relative_humidity" & .data$Operator == 3 ~ 1,
                                                  .data$Measure == "Relative_humidity" & .data$Operator == 2 ~ 1,
                                                  .data$Measure == "Temperature" & .data$Operator == 3 ~ 1,
                                                  .data$Measure == "Temperature" & .data$Operator == 2 ~ 1,
                                                  TRUE ~ as.numeric(.data$Operator)),
                      Measure = dplyr::case_when(.data$Measure == "Wind_direction" & .data$Operator == 3 ~ "Wind_direction_max",
                                                 .data$Measure == "Wind_speed" & .data$Operator == 3 ~ "Wind_speed_max",
                                                 TRUE ~ as.character(.data$Measure))) %>%
        dplyr::select(-c(.data$IDSensor, .data$Operator)) %>%
        tidyr::pivot_wider(names_from = .data$Measure, values_from = .data$Value,
                           values_fn = function(x) mean(x,na.rm=T)) %>%
        dplyr::mutate(Wind_direction = round(.data$Wind_direction,0),
                      Wind_direction_max = round(.data$Wind_direction_max,0)) %>%
        dplyr::mutate_all(list( ~ dplyr::na_if(., -9999)))
    }

    Meteo[is.na(Meteo)] <- NA
    Meteo[is.nan_df(Meteo)] <- NA

    if (verbose==T) {
      cat("Processing data: ended at", as.character(Sys.time()), "\n")
    }

    structure(list(Meteo = Meteo))
    attr(Meteo, "class") <- c("ARPALdf","tbl_df","tbl","data.frame")

    return(Meteo)
  }
