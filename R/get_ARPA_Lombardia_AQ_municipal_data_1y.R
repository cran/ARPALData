#' @keywords internal
#' @noRd

get_ARPA_Lombardia_AQ_municipal_data_1y <-
  function(ID_station = NULL, Year = 2021, Var_vec = NULL, by_sensor = F, verbose = T) {

    ### Registry
    Metadata <- AQ_municipal_metadata_reshape()
    Metadata <- Metadata %>%
      dplyr::select(-c(.data$Province,.data$DateStart,.data$DateStop))

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

    url <- url_dataset_year(Stat_type = "AQ_municipal", Year = Year)

    if (Year != 2023) {
      if (verbose==T) {
        cat("Downloading data from ARPA Lombardia: started at", as.character(Sys.time()), "\n")
      }
      zip_file <- tempfile(fileext = ".zip")
      download.file(url = url, destfile = zip_file, mode = "wb")
      if (verbose==T) {
        cat("Importing data: started at", as.character(Sys.time()), "\n")
      }
      Aria <- tibble::tibble(data.table::fread(unzip(zip_file, files = paste0("dati_stime_comunali_",Year,".csv"))))
      if (verbose==T) {
        cat("Processing data: started at", as.character(Sys.time()), "\n")
      }
      Aria <- Aria %>%
        dplyr::select(IDSensor = .data$IdSensore, Date = .data$Data, Value = .data$Valore,
                      Operator = .data$idOperatore) %>%
        dplyr::mutate(Date = lubridate::dmy_hms(.data$Date),
                      Operator = dplyr::case_when(.data$Operator == 1 ~ "mean",
                                                  .data$Operator == 3 ~ "max",
                                                  .data$Operator == 11 ~ "max_8h",
                                                  .data$Operator == 12 ~ "max_day"))
    } else {
      if (verbose==T) {
        cat("Downloading and importing data from ARPA Lombardia: started at", as.character(Sys.time()), "\n")
      }
      ## Current month
      Aria_curr_month <- RSocrata::read.socrata("https://www.dati.lombardia.it/api/odata/v4/ysm5-jwrn")
      Aria_curr_month <- Aria_curr_month %>%
        dplyr::select(IDSensor = .data$idsensore, Date = .data$data, Value = .data$valore,
                      Operator = .data$idoperatore) %>%
        dplyr::mutate(Date = lubridate::ymd(.data$Date),
                      Date = lubridate::dmy_hms(paste(lubridate::day(.data$Date),lubridate::month(.data$Date),
                                                      lubridate::year(.data$Date), "00", "00", "00", sep="-")),
                      Operator = dplyr::case_when(.data$Operator == 1 ~ "mean",
                                                  .data$Operator == 3 ~ "max",
                                                  .data$Operator == 11 ~ "max_8h",
                                                  .data$Operator == 12 ~ "max_day"))
      ## Previous months
      zip_file <- tempfile(fileext = ".zip")
      download.file(url = url, destfile = zip_file, mode = "wb")
      if (verbose==T) {
        cat("Importing data: started at", as.character(Sys.time()), "\n")
      }
      Aria <- tibble::tibble(data.table::fread(unzip(zip_file, files = paste0("dati_stime_comunali_",Year,".csv"))))
      if (verbose==T) {
        cat("Processing data: started at", as.character(Sys.time()), "\n")
      }
      Aria <- Aria %>%
        dplyr::select(IDSensor = .data$IdSensore, Date = .data$Data, Value = .data$Valore,
                      Operator = .data$idOperatore) %>%
        dplyr::mutate(Date = lubridate::dmy_hms(.data$Date),
                      Operator = dplyr::case_when(.data$Operator == 1 ~ "mean",
                                                  .data$Operator == 3 ~ "max",
                                                  .data$Operator == 11 ~ "max_8h",
                                                  .data$Operator == 12 ~ "max_day"))
      Aria <- dplyr::bind_rows(Aria,Aria_curr_month) %>%
        dplyr::arrange(.data$Date)
    }

    file.remove(paste0("dati_stime_comunali_",Year,".csv"))

    Aria <- dplyr::right_join(Aria,Metadata, by = "IDSensor")

    if (by_sensor %in% c(1,TRUE)) {
      Aria <- Aria %>%
        dplyr::mutate(Pollutant = paste0(.data$Pollutant,"_",.data$Operator)) %>%
        dplyr::filter(!is.na(.data$Date)) %>%
        dplyr::select(-c(.data$Operator)) %>%
        dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.,-9999))) %>%
        dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::na_if(.,NaN))) %>%
        dplyr::select(.data$Date,.data$IDStation,.data$NameStation,.data$IDSensor,
                      .data$Pollutant,.data$Value)
    } else if (by_sensor %in% c(0,FALSE)) {
      Aria <- Aria %>%
        dplyr::mutate(Pollutant = paste0(.data$Pollutant,"_",.data$Operator)) %>%
        dplyr::filter(!is.na(.data$Date)) %>%
        dplyr::select(-c(.data$IDSensor,.data$Operator)) %>%
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
