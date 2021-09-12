#' @keywords internal
#' @noRd

get_ARPA_Lombardia_AQ_data_1y <-
  function(ID_station = NULL, Year = 2019, Var_vec = NULL, by_sensor = 0, verbose = T) {

    ### Registry
    Metadata <- AQ_metadata_reshape()
    Metadata <- Metadata %>%
      dplyr::select(-c(.data$Altitude,.data$Province,.data$City,
                       .data$DateStart,.data$DateStop,.data$Latitude,.data$Longitude,
                       .data$ARPA_zone,.data$ARPA_stat_type))

    ### Files names (from ARPA database)
    if (Year %in% 1996:2000) {
      file_name <- "2000.csv"
    }
    if (Year %in% 2001:2004) {
      file_name <- "2004.csv"
    }
    if (Year %in% 2005:2007) {
      file_name <- "2007.csv"
    }
    if (Year %in% 2008:2010) {
      file_name <- "2010.csv"
    }
    if (Year >= 2011) {
      file_name <- paste0(Year,".csv")
    }

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
        dplyr::filter(.data$Pollutant %in% Var_vec)
    }

    url <- url_dataset_year(Stat_type = "AQ", Year = Year)

    if (Year != 2021) {
      if (verbose==T) {
        cat("Downloading data from ARPA Lombardia: started at", as.character(Sys.time()), "\n")
      }
      zip_file <- tempfile(fileext = ".zip")
      download.file(url = url, destfile = zip_file, mode = "wb")
      if (verbose==T) {
        cat("Importing data: started at", as.character(Sys.time()), "\n")
      }
      # Opening file
      Aria <- tibble::tibble(data.table::fread(unzip(zip_file, files = file_name)))
      if (verbose==T) {
        cat("Processing data: started at", as.character(Sys.time()), "\n")
      }
      Aria <- Aria %>%
        dplyr::select(IDSensor = .data$IdSensore, Date = .data$Data, Value = .data$Valore) %>%
        dplyr::mutate(Date = lubridate::dmy_hms(.data$Date))
    } else {
      if (verbose==T) {
        cat("Downloading and importing data from ARPA Lombardia: started at", as.character(Sys.time()), "\n")
      }
      Aria <- RSocrata::read.socrata(url)
      if (verbose==T) {
        cat("Processing data: started at", as.character(Sys.time()), "\n")
      }
      Aria <- Aria %>%
        dplyr::select(IDSensor = .data$idsensore, Date = .data$data, Value = .data$valore) %>%
        dplyr::mutate(Date = lubridate::ymd_hms(.data$Date))
    }

    file.remove(file_name)

    Aria <- dplyr::right_join(Aria,Metadata, by = "IDSensor")

    if (by_sensor %in% c(1,TRUE)) {
      Aria <- Aria %>%
        dplyr::filter(!is.na(.data$Date)) %>%
        dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.,-9999))) %>%
        dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.,NaN))) %>%
        dplyr::select(.data$Date,.data$IDStation,.data$NameStation,.data$IDSensor,
                      .data$Pollutant,.data$Value)
    } else if (by_sensor %in% c(0,FALSE)) {
      Aria <- Aria %>%
        dplyr::filter(!is.na(.data$Date)) %>%
        dplyr::select(-c(.data$IDSensor)) %>%
        dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.,-9999))) %>%
        dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.,NaN))) %>%
        tidyr::pivot_wider(names_from = .data$Pollutant, values_from = .data$Value,
                           values_fn = function(x) mean(x,na.rm=T)) # Mean (without NA) of a NA vector = NaN
    }

    Aria[is.na(Aria)] <- NA
    Aria[is.nan_df(Aria)] <- NA

    if (verbose==T) {
      cat("Processing data: ended at", as.character(Sys.time()), "\n")
    }

    structure(list(Aria = Aria))
    attr(Aria, "class") <- c("ARPALdf","tbl_df","tbl","data.frame")

    return(Aria)
  }
