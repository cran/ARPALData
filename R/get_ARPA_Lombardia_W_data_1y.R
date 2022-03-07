#' @keywords internal
#' @noRd

get_ARPA_Lombardia_W_data_1y <-
  function(ID_station = NULL, Year = 2019, Var_vec = NULL, by_sensor = F, verbose = T) {

    ### Registry
    Metadata <- W_metadata_reshape()
    Metadata <- Metadata %>%
      dplyr::select(-c(.data$Altitude,.data$Province,
                       .data$DateStart,.data$DateStop,
                       .data$Latitude,.data$Longitude))

    ### Files names (from ARPA database)
    file_name <- dplyr::case_when(Year >= 2013 ~ paste0(Year,".csv"),
                                  Year %in% 2011:2012 ~ "2012.csv",
                                  Year %in% 2009:2010 ~ "2010.csv",
                                  Year %in% 2006:2008 ~ "2008.csv",
                                  Year %in% 2001:2005 ~ "2005.csv",
                                  Year %in% 1989:2000 ~ "2000.csv")

    ### Checks if ID_station is valid (in the list of active stations)
    '%notin%' <- Negate('%in%')
    if (!is.null(ID_station) & all(ID_station %notin% Metadata$IDStation)) {
      stop("ID_station NOT in the list of active stations. Change ID_station or use ID_station = NULL",
           call. = FALSE)
    }

    if (!is.null(ID_station)) {
      Metadata <- Metadata %>%
        dplyr::filter(.data$IDStation %in% ID_station)
    }
    if (!is.null(Var_vec)) {
      Metadata <- Metadata %>%
        dplyr::filter(.data$Measure %in% Var_vec)
    }

    url <- url_dataset_year(Stat_type = "W", Year = Year)

    if (Year != 2022) {
      if (verbose==T) {
        cat("Downloading data from ARPA Lombardia: started at", as.character(Sys.time()), "\n")
      }
      zip_file <- tempfile(fileext = ".zip")
      download.file(url = url, destfile = zip_file, mode = "wb")
      if (verbose==T) {
        cat("Importing data: started at", as.character(Sys.time()), "\n")
      }
      Meteo <- tibble::tibble(data.table::fread(unzip(zip_file, files = file_name)))
      if (verbose==T) {
        cat("Processing data: started at", as.character(Sys.time()), "\n")
      }
      Meteo <- Meteo %>%
        dplyr::select(IDSensor = .data$IdSensore, Date = .data$Data, Value = .data$Valore,
                      Operator = .data$idOperatore) %>%
        dplyr::mutate(Date = lubridate::dmy_hms(.data$Date),
                      IDSensor = as.numeric(.data$IDSensor))
    } else {
      if (verbose==T) {
        cat("Downloading data from ARPA Lombardia: started at", as.character(Sys.time()), "\n")
      }
      Meteo_last_month <- RSocrata::read.socrata("https://www.dati.lombardia.it/api/odata/v4/647i-nhxk")
      zip_file <- tempfile(fileext = ".zip")
      download.file(url = url, destfile = zip_file, mode = "wb")
      if (verbose==T) {
        cat("Importing data: started at", as.character(Sys.time()), "\n")
      }
      Meteo <- tibble::tibble(data.table::fread(unzip(zip_file, files = file_name)))
      if (verbose==T) {
        cat("Processing data: started at", as.character(Sys.time()), "\n")
      }
      Meteo <- Meteo %>%
        dplyr::select(IDSensor = .data$IdSensore, Date = .data$Data, Value = .data$Valore,
                      Operator = .data$idOperatore) %>%
        dplyr::mutate(Date = lubridate::dmy_hms(.data$Date))
      Meteo_last_month <- Meteo_last_month %>%
        dplyr::select(IDSensor = .data$idsensore, Date = .data$data, Value = .data$valore,
                      Operator = .data$idoperatore)
      Meteo <- dplyr::bind_rows(Meteo,Meteo_last_month)
      Meteo <- Meteo %>%
        dplyr::mutate(IDSensor = as.numeric(.data$IDSensor))
    }

    file.remove(file_name)

    Meteo <- dplyr::right_join(Meteo,Metadata, by = "IDSensor")

    if (by_sensor %in% c(1,TRUE)) {
      Meteo <- Meteo %>%
        dplyr::filter(!is.na(.data$Date)) %>%
        dplyr::mutate(Operator = dplyr::case_when(.data$Measure == "Relative_humidity" & .data$Operator == 3 ~ 1,
                                                  .data$Measure == "Relative_humidity" & .data$Operator == 2 ~ 1,
                                                  .data$Measure == "Temperature" & .data$Operator == 3 ~ 1,
                                                  .data$Measure == "Temperature" & .data$Operator == 2 ~ 1,
                                                  TRUE ~ as.numeric(.data$Operator)),
                      Measure = dplyr::case_when(.data$Measure == "Wind_direction" & .data$Operator == 3 ~ "Wind_direction_gust",
                                                 .data$Measure == "Wind_speed" & .data$Operator == 3 ~ "Wind_speed_gust",
                                                 TRUE ~ as.character(.data$Measure))) %>%
        dplyr::select(-c(.data$Operator)) %>%
        dplyr::select(.data$Date,.data$IDStation,.data$NameStation,.data$IDSensor,
                      .data$Measure,.data$Value) %>%
        dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.,-9999))) %>%
        dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.,NaN)))
    } else if (by_sensor %in% c(0,FALSE)) {
      Meteo <- Meteo %>%
        dplyr::filter(!is.na(.data$Date)) %>%
        dplyr::mutate(Operator = dplyr::case_when(.data$Measure == "Relative_humidity" & .data$Operator == 3 ~ 1,
                                                  .data$Measure == "Relative_humidity" & .data$Operator == 2 ~ 1,
                                                  .data$Measure == "Temperature" & .data$Operator == 3 ~ 1,
                                                  .data$Measure == "Temperature" & .data$Operator == 2 ~ 1,
                                                  TRUE ~ as.numeric(.data$Operator)),
                      Measure = dplyr::case_when(.data$Measure == "Wind_direction" & .data$Operator == 3 ~ "Wind_direction_gust",
                                                 .data$Measure == "Wind_speed" & .data$Operator == 3 ~ "Wind_speed_gust",
                                                 TRUE ~ as.character(.data$Measure))) %>%
        dplyr::select(-c(.data$IDSensor, .data$Operator)) %>%
        tidyr::pivot_wider(names_from = .data$Measure, values_from = .data$Value,
                           values_fn = function(x) mean(x,na.rm=T)) %>%
        dplyr::mutate(dplyr::across(dplyr::matches(c("Wind_direction","Wind_direction_max")), ~ round(.x,0))) %>%
        dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.,-9999))) %>%
        dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.,NaN)))
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
