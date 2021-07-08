#' Download air quality data from ARPA Lombardia website
#'
#' More detailed description.
#'
#' @description 'get_ARPA_Lombardia_W_data' returns observed air weather measurements collected by
#' ARPA Lombardia ground detection system for Lombardy region in Northern Italy.
#' Available meteorological variables are: temperature (Celsius degrees), rainfall (cm), wind speed (m/s),
#' wind direction (degrees), relative humidity (%), global_radiation (W/m2)and snow height (cm).
#' Data are available from 2011 and are updated up to the current date.
#' For more information about the municipal data visit the section 'Monitoraggio aria' at the webpage:
#' https://www.dati.lombardia.it/stories/s/auv9-c2sj
#'
#' @param ID_station Numeric value. ID of the station to consider. Using ID_station = NULL, all the available
#' stations are selected. Default is ID_station = NULL.
#' @param Year Numeric vector. Year(s) of interest. Default is Year = 2019. Specifying more than one year the
#' code works in parallel computing (half of the available cores) using parLapply() function.
#' @param Frequency Temporal aggregation frequency. It can be "10mins", "hourly", "daily", "weekly",
#' "monthly". Default is Frequency = "10mins"
#' @param Var_vec Character vector of variables to aggregate. If NULL (default) all the variables are averaged,
#' expect for 'Temperature' and 'Snow_height' which are summed.
#' @param Fns_vec Character vector of aggregation function to apply to the selected variables. Available functions
#'  are mean, median, min, max, sum and qPP (for the PP-th percentile).
#' @param by_sensor Logic value (0 or 1). If 'by_sensor=1', the function returns the observed concentrations
#' by sensor code, while if 'by_sensor=0' (default) it returns the observed concentrations by station.
#' @param verbose Logic value (T or F). Toggle warnings and messages. If 'verbose=T' (default) the function
#' prints on the screen some messages describing the progress of the tasks. If 'verbose=F' any message about
#' the progression is suppressed.
#'
#'
#' @return A data frame of class 'data.frame' and 'ARPALdf'. The object is fully compatible with Tidyverse.
#'
#' @examples
#' \donttest{
#' ## Download all the weather measurements at station 100 during 2020. Data have 10 minutes frequency.
#' get_ARPA_Lombardia_W_data(ID_station = 100, Year = 2020, Frequency = "10mins")
#' ## Download all the weather measurements at station 100 during 2020. Data have 10 minutes frequency.
#' ## Data are reported by sensor.
#' get_ARPA_Lombardia_W_data(ID_station = 100, Year = 2020, by_sensor = 1)
#' }
#'
#' @export

get_ARPA_Lombardia_W_data <-
  function(ID_station = NULL, Year = 2019, Frequency = "10mins", Var_vec = NULL, Fns_vec = NULL,by_sensor = 0,verbose=T) {

    if (length(Year) == 1) {
      Meteo <- get_ARPA_Lombardia_W_data_1y(ID_station = ID_station, Year = Year, Var_vec = Var_vec, verbose=verbose)
      attr(Meteo, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
    } else {
      cl <- parallel::makeCluster(parallel::detectCores()/2-2, type = 'PSOCK')
      doParallel::registerDoParallel(cl)
      parallel::clusterExport(cl, varlist = c("get_ARPA_Lombardia_W_data_1y",
                                              "Time_aggregate",
                                              "W_metadata_reshape",
                                              "url_dataset_year",
                                              "%>%"),
                              envir=environment())
      if (verbose==T) {
        cat("Parallel (",parallel::detectCores()/2-2,"cores) download, import and process of ARPA Lombardia data: started at", as.character(Sys.time()), "\n")
      }
      Meteo <- dplyr::bind_rows(parallel::parLapply(cl = cl,
                                                    X = Year,
                                                    fun = get_ARPA_Lombardia_W_data_1y,
                                                    ID_station = ID_station,
                                                    Var_vec = Var_vec))
      if (verbose==T) {
        cat("Parallel download, import and process of ARPA data: ended at", as.character(Sys.time()), "\n")
      }
      parallel::stopCluster(cl)
      attr(Meteo, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
    }

    if (is.null(Var_vec) & is.null(Fns_vec)) {
      vv <- c("Rainfall","Temperature","Relative_humidity","Global_radiation","Water_height",
              "Snow_height","Wind_speed","Wind_speed_max","Wind_direction","Wind_direction_max")
      vv <- vv[vv %in% names(Meteo)]
      fv <- ifelse(vv == "Rainfall" | vv == "Snow_height", "sum", "mean")
    } else {
      vv <- Var_vec
      fv <- Fns_vec
    }

    # Checks if all the variables are available for the selected stations
    if (all(dplyr::all_of(vv) %in% names(Meteo)) == F) {
      stop("One ore more variables are not avaiable for the selected stations! Change the values of 'Var_vec'",
           call. = FALSE)
    }

    # Checks if by_sensor setup properly
    '%notin%' <- Negate('%in%')
    if (by_sensor %notin% c(0,1,FALSE,TRUE)) {
      stop("Wrong setup for 'by_sensor'. Use 1 or 0 or TRUE or FALSE.",
           call. = FALSE)
    }

    if (by_sensor %in% c(0,FALSE)) {
      # Aggregating dataset
      if (Frequency != "10mins") {
        if (verbose==T) {
          cat("Aggregating ARPA Lombardia data: started started at", as.character(Sys.time()), "\n")
        }
        Meteo <- Meteo %>%
          Time_aggregate(Frequency = Frequency, Var_vec = Var_vec, Fns_vec = Fns_vec, verbose = verbose) %>%
          dplyr::arrange(.data$NameStation, .data$Date)
      } else {
        Meteo <- Meteo %>%
          dplyr::select(.data$Date,.data$IDStation,.data$NameStation,vv) %>%
          dplyr::arrange(.data$NameStation, .data$Date)
      }

      # Regularizing dataset: same number of timestamps for each station and variable
      if (verbose==T) {
        cat("Regularizing ARPA data: started started at", as.character(Sys.time()), "\n")
      }
      freq_unit <- dplyr::case_when(Frequency == "10mins" ~ "10 min",
                                    Frequency == "hourly" ~ "hours",
                                    Frequency == "daily" ~ "days",
                                    Frequency == "weekly" ~ "weeks",
                                    Frequency == "monthly" ~ "months")
      Meteo <- Meteo %>%
        dplyr::arrange(.data$Date)
      dt <- seq(min(Meteo$Date),max(Meteo$Date), by = freq_unit)
      st <- unique(Meteo$IDStation)
      grid <- data.frame(tidyr::expand_grid(dt,st))
      st_n <- Meteo %>%
        dplyr::distinct(.data$IDStation,.data$NameStation) %>%
        dplyr::filter(!is.na(.data$IDStation),!is.na(.data$NameStation))
      colnames(grid) <- c("Date","IDStation")
      grid <- dplyr::left_join(grid,st_n,by="IDStation")
      Meteo <- dplyr::left_join(grid,Meteo, by=c("Date","IDStation","NameStation"))

      if (verbose==T) {
        cat("Processing ARPA Lombardia data: ended at", as.character(Sys.time()), "\n")
      }

      structure(list(Meteo = Meteo))
      attr(Meteo, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
      attr(Meteo, "frequency") <- Frequency
      attr(Meteo, "units") <- freq_unit
    } else if (by_sensor %in% c(1,TRUE)) {
      structure(list(Meteo = Meteo))
      attr(Meteo, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
      attr(Meteo, "frequency") <- "10mins"
      attr(Meteo, "units") <- "10 min"
    }

    return(Meteo)
  }
