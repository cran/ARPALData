#' Download weather/meteorological data from ARPA Lombardia website
#'
#' More detailed description.
#'
#' @description 'get_ARPA_Lombardia_W_data' returns observed air weather measurements collected by
#' ARPA Lombardia ground detection system for Lombardy region in Northern Italy.
#' Available meteorological variables are: temperature (Celsius degrees), rainfall (cm), wind speed (m/s),
#' wind direction (degrees), relative humidity (%), global_radiation (W/m2)and snow height (cm).
#' Data are available from 1989 and are updated up to the current date.
#' For more information about the municipal data visit the section 'Monitoraggio aria' at the webpage:
#' https://www.dati.lombardia.it/stories/s/auv9-c2sj
#'
#' @param ID_station Numeric value. ID of the station to consider. Using ID_station = NULL, all the available
#' stations are selected. Default is ID_station = NULL.
#' @param Year Numeric vector. Year(s) of interest. Default is Year = 2022. Specifying more than one year the
#' code works in parallel computing (half of the available cores) using parLapply() function.
#' @param Frequency Temporal aggregation frequency. It can be "10mins", "hourly", "daily", "weekly",
#' "monthly". Default is Frequency = "10mins"
#' @param Var_vec Character vector of variables to aggregate. If NULL (default) all the variables are averaged,
#' except for 'Temperature' and 'Snow_height', which are cumulated.
#' @param Fns_vec Character vector of aggregation function to apply to the selected variables.
#' Available functions are mean, median, min, max, sum, qPP (PP-th percentile), sd, var,
#' vc (variability coefficient), skew (skewness) and kurt (kurtosis). Attention: for Wind Speed and
#' Wind Speed Gust only mean, min and max are available; for Wind Direction and Wind Direction Gust
#' only mean is available.
#' @param by_sensor Logic value (T or F). If 'by_sensor=T', the function returns the observed concentrations
#' by sensor code, while if 'by_sensor=F' (default) it returns the observed concentrations by station.
#' @param parallel Logic value (T or F). If 'parallel=T' (default), data downloading is performed using parallel computing
#' (socketing), while if 'parallel=F' (default) the download is performed serially. 'parallel=T' works only when
#' 'Year' is a vector with multiple values, i.e. for a single year the serial computing is performed.
#' @param verbose Logic value (T or F). Toggle warnings and messages. If 'verbose=T' (default) the function
#' prints on the screen some messages describing the progress of the tasks. If 'verbose=F' any message about
#' the progression is suppressed.
#'
#'
#' @return A data frame of class 'data.frame' and 'ARPALdf'. The object is fully compatible with Tidyverse.
#'
#' @examples
#' \donttest{
#' ## Download all the (10 minutes frequency) weather measurements at station 100 during 2022.
#' get_ARPA_Lombardia_W_data(ID_station = 100, Year = 2022, Frequency = "10mins")
#' ## Download all the (daily frequency) weather measurements at station 100 during 2023.
#' get_ARPA_Lombardia_W_data(ID_station = 100, Year = 2023, Frequency = "daily")
#' }
#'
#' @export

get_ARPA_Lombardia_W_data <-
  function(ID_station = NULL, Year = 2022, Frequency = "10mins", Var_vec = NULL, Fns_vec = NULL,by_sensor = F,parallel = T,verbose=T) {

    ##### Define %notin%
    '%notin%' <- Negate('%in%')

    ##### Control checks on Var_vec and Fns_vec
    # Wind_speed and Wind_direction must be selected together
    if (!is.na(match("Wind_speed",Var_vec)) & is.na(match("Wind_direction",Var_vec))) {
      stop("It's not possible to select only Wind_speed: also Wind_direction must be included in the list of selected variables! Change the values of 'Var_vec' and 'Var_funs'",
           call. = FALSE)
    }
    if (is.na(match("Wind_speed",Var_vec)) & !is.na(match("Wind_direction",Var_vec))) {
      stop("It's not possible to select only Wind_direction: also Wind_speed must be included in the list of selected variables! Change the values of 'Var_vec' and 'Var_funs'",
           call. = FALSE)
    }
    # Wind direction can only be averaged
    if(sum(Var_vec %in% c("Wind_direction","Wind_direction_gust") & Fns_vec != "mean") > 0) {
      stop("Error: on Wind_direction and Wind_direction_gust is possible to calculate only the average value. Use 'mean' in 'Fns_vec.'",
           call. = FALSE)
    }
    # Wind speed can only be averaged, maximized or minimized
    if(sum(Var_vec %in% c("Wind_speed","Wind_speed_gust") & Fns_vec %notin% c("mean","min","max")) > 0) {
      stop("Error: on Wind_speed and Wind_speed_gust is possible to calculate only mean, max or min values. Use 'mean' or 'max' or 'min' in 'Fns_vec.'",
           call. = FALSE)
    }


    ##### Downloading data
    if (length(Year) == 1) {
      Meteo <- get_ARPA_Lombardia_W_data_1y(ID_station = ID_station, Year = Year, Var_vec = Var_vec, verbose=verbose)
    } else {
      if (parallel == T) {
        cl <- parallel::makeCluster(min(length(Year),parallel::detectCores()/2), type = 'PSOCK')
        doParallel::registerDoParallel(cl)
        parallel::clusterExport(cl, varlist = c("get_ARPA_Lombardia_W_data_1y",
                                                "Time_aggregate",
                                                "W_metadata_reshape",
                                                "url_dataset_year",
                                                "%>%"),
                                envir=environment())
        if (verbose==T) {
          cat("Parallel (",min(length(Year),parallel::detectCores()/2),"cores) download, import and process of ARPA Lombardia data: started at", as.character(Sys.time()), "\n")
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
      } else {
        Meteo_d <- vector("list", length = length(Year))
        for (j in 1:length(Year)) {
          Meteo_d[[j]] <- get_ARPA_Lombardia_W_data_1y(ID_station = ID_station, Year = Year[j], Var_vec = Var_vec,
                                                       by_sensor = by_sensor, verbose = verbose)
        }
        Meteo <- dplyr::bind_rows(Meteo_d)
      }
    }
    attr(Meteo, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")

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
        dplyr::arrange(.data$Date) %>%
        dplyr::filter(lubridate::year(.data$Date) %in% Year) %>%
        tidyr::pivot_longer(cols = -c(.data$Date,.data$IDStation,.data$NameStation),
                            names_to = "Measure", values_to = "Value") %>%
        tidyr::pivot_wider(names_from = .data$Date, values_from = .data$Value) %>%
        tidyr::pivot_longer(cols = -c(.data$Measure,.data$IDStation,.data$NameStation),
                            names_to = "Date", values_to = "Value") %>%
        tidyr::pivot_wider(names_from = .data$Measure, values_from = .data$Value)


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
