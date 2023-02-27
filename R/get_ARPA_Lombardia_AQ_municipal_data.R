#' Download air quality data at manucipal from ARPA Lombardia website
#'
#' More detailed description.
#'
#' @description 'get_ARPA_Lombardia_AQ_municipal_data' returns the air quality levels at municipal level estimated by
#' ARPA Lombardia using a physico-chemical model which simulates air quality based on weather and geo-physical
#' variables. For each municipality of Lombardy, ARPA estimates the average (NO2_mean) and maximum daily (NO2_max_day)
#' level of NO2, the daily maximum (Ozone_max_day) and the 8-hours moving window maximum (Ozone_max_8h) of Ozone
#' and the average levels of PM10 (PM10_mean) and PM2.5 (PM2.5_mean).
#' Data are available from 2017 and are updated up to the current date.
#' For more information about the municipal data visit the section 'Stime comunali dell'aria' at the webpage:
#' https://www.dati.lombardia.it/stories/s/auv9-c2sj
#'
#' @param ID_station Numeric value. ID of the station to consider. Using ID_station = NULL, all the available
#' stations are selected. Default is ID_station = NULL.
#' @param Year Numeric vector. Year(s) of interest. Default is Year = 2020. Specifying more than one year the
#' code works in parallel computing (half of the available cores) using parLapply() function.
#' @param Frequency Temporal aggregation frequency. It can be "daily", "weekly", "monthly" or "yearly".
#' Default is Frequency = "daily".
#' @param Var_vec Character vector of variables to aggregate. If NULL (default) all the variables are averaged.
#' @param Fns_vec Character vector of aggregation function to apply to the selected variables.
#' Available functions are mean, median, min, max, sum, qPP (PP-th percentile), sd, var,
#' vc (variability coefficient), skew (skewness) and kurt (kurtosis).
#' @param by_sensor Boolean. If 'by_sensor=1', the function returns the observed concentrations by sensor code, while
#' if 'by_sensor=0' (default) it returns the observed concentrations by station.
#' @param parallel Logic value (T or F). If 'parallel=T' (default), data downloading is performed using parallel computing
#' (socketing), while if 'parallel=F' (default) the download is performed serially. 'parallel=T' works only when
#' 'Year' is a vector with multiple values, i.e. for a single year the serial computing is performed.
#' @param verbose Logic value (T or F). Toggle warnings and messages. If 'verbose=T' (default) the function
#' prints on the screen some messages describing the progress of the tasks. If 'verbose=F' any message about
#' the progression is suppressed.
#'
#' @return A data frame of class 'data.frame' and 'ARPALdf'. The object is fully compatible with Tidyverse.
#' The column 'NameStation' identifies the name of each municipality. The column 'IDStation' is an ID code
#' (assigned from ARPA) uniquely identifying each municipality.
#'
#' @examples
#' \donttest{
#' ## Download daily concentrations at municipal levels observed in 2020
#' ## for all the cities in Lombardy
#' get_ARPA_Lombardia_AQ_municipal_data(ID_station=NULL,Year=2020,
#'      Frequency="daily")
#' ## Download monthly concentrations of NO2 (average and maximum) observed in 2021
#' ## at city number 100451.
#' get_ARPA_Lombardia_AQ_municipal_data(ID_station=100451,Year=2021,
#'       Frequency="monthly",Var_vec=c("NO2_mean","NO2_mean"),Fns_vec=c("mean","max"))
#' ## Download daily concentrations observed in 2021 at city number 100451.
#' ## Data are reported by sensor.
#' get_ARPA_Lombardia_AQ_municipal_data(ID_station=100451,Year=2021,by_sensor = 1)
#' }
#'
#' @export

get_ARPA_Lombardia_AQ_municipal_data <-
  function(ID_station = NULL, Year = 2020, Frequency = "daily", Var_vec = NULL, Fns_vec = NULL,by_sensor = F,parallel=T,verbose=T) {

    ### Checks
    stopifnot("Frequency cannot be hourly as municipal estimates are provided on a daily basis" = (Frequency != "hourly") == T)

    if (length(Year) == 1) {
      Aria <- get_ARPA_Lombardia_AQ_municipal_data_1y(ID_station = ID_station, Year = Year, Var_vec = Var_vec, verbose=verbose)
    } else {
      if (parallel == T) {
        cl <- parallel::makeCluster(min(length(Year),parallel::detectCores()/2), type = 'PSOCK')
        doParallel::registerDoParallel(cl)
        parallel::clusterExport(cl, varlist = c("get_ARPA_Lombardia_AQ_municipal_data_1y",
                                                "Time_aggregate",
                                                "AQ_metadata_reshape",
                                                "url_dataset_year",
                                                "%>%"),
                                envir=environment())
        if (verbose==T) {
          cat("Parallel (",min(length(Year),parallel::detectCores()/2),"cores) download, import and process of ARPA Lombardia data: started at", as.character(Sys.time()), "\n")
        }
        Aria <- dplyr::bind_rows(parallel::parLapply(cl = cl,
                                                     X = Year,
                                                     fun = get_ARPA_Lombardia_AQ_municipal_data_1y,
                                                     ID_station = ID_station,
                                                     Var_vec = Var_vec))
        if (verbose==T) {
          cat("Parallel download, import and process of ARPA Lombardia data: ended at", as.character(Sys.time()), "\n")
        }
        parallel::stopCluster(cl)
      }
    }
    attr(Aria, "class") <- c("ARPALdf","ARPALdf_AQ","tbl_df","tbl","data.frame")

    if (is.null(Var_vec) & is.null(Fns_vec)) {
      vv <- c("NO2_mean","NO2_max_day","Ozone_max_8h","Ozone_max_day","PM10_mean","PM2.5_mean")
      vv <- vv[vv %in% names(Aria)]
      fv <- rep("mean",length(vv))
    } else {
      vv <- Var_vec
      fv <- Fns_vec
    }

    # Checks if all the pollutants are available for the selected stations
    if (all(dplyr::all_of(vv) %in% names(Aria)) == F) {
      stop("One ore more pollutants are not avaiable for the selected stations! Change the values of 'Var_vec'",
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
      if (Frequency != "daily") {
        if (verbose==T) {
          cat("Aggregating ARPA Lombardia data: started started at", as.character(Sys.time()), "\n")
        }
        Aria <- Aria %>%
          Time_aggregate(Frequency = Frequency, Var_vec = Var_vec, Fns_vec = Fns_vec, verbose = verbose) %>%
          dplyr::arrange(.data$NameStation, .data$Date)
      } else {
        Aria <- Aria %>%
          dplyr::select(.data$Date,.data$IDStation,.data$NameStation,vv) %>%
          dplyr::arrange(.data$NameStation, .data$Date)
      }

      # Regularizing dataset: same number of timestamps for each station and variable
      if (verbose==T) {
        cat("Regularizing ARPA Lombardia data: started started at", as.character(Sys.time()), "\n")
      }
      freq_unit <- dplyr::case_when(Frequency == "daily" ~ "days",
                                    Frequency == "weekly" ~ "weeks",
                                    Frequency == "monthly" ~ "months",
                                    Frequency == "yearly" ~ "years")
      Aria <- Aria %>%
        dplyr::arrange(.data$Date)
      dt <- seq(min(Aria$Date),max(Aria$Date), by = freq_unit)
      st <- unique(Aria$IDStation)
      grid <- data.frame(tidyr::expand_grid(dt,st))
      st_n <- Aria %>%
        dplyr::distinct(.data$IDStation,.data$NameStation) %>%
        dplyr::filter(!is.na(.data$IDStation),!is.na(.data$NameStation))
      colnames(grid) <- c("Date","IDStation")
      grid <- dplyr::left_join(grid,st_n,by="IDStation")
      Aria <- dplyr::left_join(grid,Aria, by=c("Date","IDStation","NameStation"))

      if (verbose==T) {
        cat("Processing ARPA Lombardia data: ended at", as.character(Sys.time()), "\n")
      }

      structure(list(Aria = Aria))
      attr(Aria, "class") <- c("ARPALdf","ARPALdf_AQ_mun","tbl_df","tbl","data.frame")
      attr(Aria, "frequency") <- Frequency
      attr(Aria, "units") <- freq_unit
    } else if (by_sensor %in% c(1,TRUE)) {
      structure(list(Aria = Aria))
      attr(Aria, "class") <- c("ARPALdf","ARPALdf_AQ","tbl_df","tbl","data.frame")
      attr(Aria, "frequency") <- "hourly"
      attr(Aria, "units") <- "hours"
    }

    return(Aria)
  }
