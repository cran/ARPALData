#' Download weather/meteorological data from ARPA Lombardia website
#'
#' More detailed description.
#'
#' @description 'get_ARPA_Lombardia_W_data' returns observed air weather measurements collected by
#' ARPA Lombardia ground detection system for Lombardy region in Northern Italy.
#' Available meteorological variables are: temperature (Celsius degrees), rainfall (mm), wind speed (m/s),
#' wind direction (degrees), relative humidity (%), global solar radiation (W/m2), and snow height (cm).
#' Data are available from 1989 and are updated up to the current date.
#' For more information about the municipal data visit the section 'Monitoraggio aria' at the webpage:
#' https://www.dati.lombardia.it/stories/s/auv9-c2sj
#'
#' @param ID_station Numeric value. ID of the station to consider. Using ID_station = NULL, all the available
#' stations are selected. Default is ID_station = NULL.
#' @param Date_begin Character vector of the first date-time to download. Format can be either "YYYY-MM-DD" or "YYYY-MM-DD hh:mm:ss". Default is Date_begin = "2022-01-01".
#' @param Date_end Character vector of the last date-time to download. Format can be either "YYYY-MM-DD" or "YYYY-MM-DD hh:mm:ss". Default is Date_end = "2022-12-31".
#' @param Frequency Temporal aggregation frequency. It can be "10mins", "hourly", "daily", "weekly",
#' "monthly". Default is Frequency = "10mins"
#' @param Var_vec Character vector of variables to aggregate. If NULL (default) all the variables are averaged,
#' except for 'Temperature' and 'Snow_height', which are cumulated.
#' @param Fns_vec Character vector of aggregation function to apply to the selected variables.
#' Available functions are mean, median, min, max, sum, qPP (PP-th percentile), sd, var,
#' vc (variability coefficient), skew (skewness) and kurt (kurtosis). Attention: for Wind Speed and
#' Wind Speed Gust only mean, min and max are available; for Wind Direction and Wind Direction Gust
#' only mean is available.
#' @param by_sensor Logic value (TRUE or FALSE). If 'by_sensor = TRUE', the function returns the observed concentrations
#' by sensor code, while if 'by_sensor = FALSE' (default) it returns the observed concentrations by station.
#' @param verbose Logic value (TRUE or FALSE). Toggle warnings and messages. If 'verbose = TRUE' (default) the function
#' prints on the screen some messages describing the progress of the tasks. If 'verbose = FALSE' any message about
#' the progression is suppressed.
#' @param parallel Logic value (TRUE or FALSE). If 'parallel = FALSE' (default), data downloading is performed using a sequential/serial approach and additional parameters 'parworkers' and 'parfuturetype' are ignored.
#' When 'parallel = TRUE', data downloading is performed using parallel computing through the Futureverse setting.
#' More detailed information about parallel computing in the Futureverse can be found at the following webpages:
#' https://future.futureverse.org/ and https://cran.r-project.org/web/packages/future.apply/vignettes/future.apply-1-overview.html
#' @param parworkers Numeric integer value. If 'parallel = TRUE' (parallel mode active), the user can declare the number of parallel workers to be activated using 'parworkers = integer number'. By default ('parworkers = NULL'), the number of active workers is half of the available local cores.
#' @param parfuturetype Character vector. If 'parallel = TRUE' (parallel mode active), the user can declare the parallel strategy to be used according to the Futureverse syntax through 'parfuturetype'. By default, the 'multisession' (background R sessions on local machine) is used. In alternative, the 'multicore' (forked R processes on local machine. Not supported by Windows and RStudio) setting can be used.
#'
#'
#'
#' @return A data frame of class 'data.frame' and 'ARPALdf'. The object is fully compatible with Tidyverse.
#'
#' @examples
#' \donttest{
#' ## Download all the (10 minutes frequency) weather measurements at station 100
#' ## between August 2021 and December 2022.
#' if (require("RSocrata")) {
#'     get_ARPA_Lombardia_W_data(ID_station = 100, Date_begin = "2021-08-01",
#'           Date_end = "2022-12-31", Frequency = "10mins")
#' }
#' ## Download all the (daily frequency) weather measurements at station 1974 during 2022
#' if (require("RSocrata")) {
#'     get_ARPA_Lombardia_W_data(ID_station = 1974, Date_begin = "2022-01-01",
#'           Date_end = "2022-12-31", Frequency = "daily")
#' }
#' }
#'
#' @export

get_ARPA_Lombardia_W_data <-
  function(ID_station = NULL, Date_begin = "2021-01-01", Date_end = "2022-12-31",
           Frequency = "10mins", Var_vec = NULL, Fns_vec = NULL, by_sensor = FALSE, verbose = TRUE,
           parallel = FALSE, parworkers = NULL, parfuturetype = "multisession") {

    ###  Welcome message
    if (verbose == TRUE) {
      cat("Retrieving desired ARPA Lombardia dataset: started at", as.character(Sys.time()), "\n")
    }

    ############################
    ########## Checks ##########
    ############################

    ##### Check for internet connection
    if(!curl::has_internet()) {
      message("Internet connection not available at the moment.\nPlease check your internet connection. If the problem persists, please contact the package maintainer.")
      return(invisible(NULL))
    }

    ##### Check if package 'RSocrata' is installed
    # See: https://r-pkgs.org/dependencies-in-practice.html#sec-dependencies-in-suggests-r-code
    rlang::check_installed("RSocrata",
                           reason = "Package \"RSocrata\" must be installed to download data from ARPA Lombardia Open Database.")

    ##### Check if Futureverse is installed
    rlang::check_installed("future",
                           reason = "Package \"future\" must be installed to download (parallel) data from ARPA Lombardia Open Database.")
    rlang::check_installed("future.apply",
                           reason = "Package \"future\" must be installed to download (parallel) data from ARPA Lombardia Open Database.")

    ##### Define %notin%
    '%notin%' <- Negate('%in%')

    ##### Check if package 'RSocrata' is installed
    # See: https://r-pkgs.org/dependencies-in-practice.html#sec-dependencies-in-suggests-r-code
    rlang::check_installed("RSocrata", reason = "Package \"RSocrata\" must be installed to download data from ARPA Lombardia Open Database.")

    ##### Checks if by_sensor setup properly
    if (by_sensor %notin% c(0,1,FALSE,TRUE)) {
      stop("Wrong setup for 'by_sensor'. Use 1 or 0 or TRUE or FALSE.",
           call. = FALSE)
    }

    ##### Checks if parallel setup properly
    if (parallel %notin% c(FALSE,TRUE)) {
      stop("Wrong setup for 'parallel'. Use TRUE or FALSE.",
           call. = FALSE)
    }
    if (parfuturetype %notin% c("multisession","multicore")) {
      stop("Wrong setup for 'parallel'. Use TRUE or FALSE.",
           call. = FALSE)
    }

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



    ######################################
    ########## Downloading data ##########
    ######################################

    ### Registry
    Metadata <- W_metadata_reshape()
    Metadata <- Metadata %>%
      dplyr::select(-c(.data$Altitude,.data$Province,
                       .data$DateStart,.data$DateStop,
                       .data$Latitude,.data$Longitude))

    ### Checks if ID_station is valid (in the list of active stations)
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

    ##### Splitting strategy for improving download speed
    n_blocks <- 12
    # Check dates format
    if (is.null(lubridate::guess_formats(x = Date_begin, orders = c("ymd HMS","ymd")))) {
      stop("Wrong format for 'Date_begin'. Use 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss'", call. = FALSE)
    }
    if (is.null(lubridate::guess_formats(x = Date_end, orders = c("ymd HMS","ymd")))) {
      stop("Wrong format for 'Date_end'. Use 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss'", call. = FALSE)
    }
    if (is.na(lubridate::ymd_hms(Date_begin, quiet = TRUE))) {
      Date_begin <- lubridate::ymd_hms(paste0(Date_begin," 00:00:00"))
    }
    if (is.na(lubridate::ymd_hms(Date_end, quiet = TRUE))) {
      Date_end <- lubridate::ymd_hms(paste0(Date_end," 23:55:00"))
    }
    ### Check for the presence of breaking dates (URLs change in several years)
    if (lubridate::year(Date_begin) == lubridate::year(Date_end)) {
      break_years <- lubridate::year(Date_begin)
    } else {
      break_years <- seq(from = lubridate::year(Date_begin), to = lubridate::year(Date_end), by = 1)
    }

    ##### Check online availability of the resources for the specified years
    if (verbose == TRUE) {
      cat("Checking availability of online resources: started at", as.character(Sys.time()), "\n")
    }
    URLs <- res_check <- numeric(length = length(break_years))
    for (yr in 1:length(break_years)) {
      URLs[yr] <- url <- url_dataset_year(Stat_type = "W_check", Year = break_years[yr])
      temp <- tempfile()
      res <- suppressWarnings(try(curl::curl_fetch_disk(url, temp), silent = TRUE))
      URLs[yr] <- url <- url_dataset_year(Stat_type = "W", Year = break_years[yr])
      if(res$status_code != 200) {
        message(paste0("The internet resource for year ", break_years[yr]," is not available at the moment. Status code: ",res$status_code,".\nPlease, try later. If the problem persists, please contact the package maintainer."))
      } else {
        res_check[yr] <- 1
      }
    }
    if (verbose == TRUE) {
      if (sum(res_check) == length(break_years)) {
        message("All the online resources are available.\n")
      }
      if (sum(res_check) > 0 & sum(res_check) < length(break_years)) {
        message("Part of the required online resources are not available. Please, try with a new request.\n")
        return(invisible(NULL))
      }
      if (sum(res_check) == 0) {
        message("None of the required online resources are available. Please, try with a new request.\n")
        return(invisible(NULL))
      }
    }








    ### Downloading data
    if (verbose == TRUE) {
      cat("Downloading data: started at", as.character(Sys.time()), "\n")
    }

    ##### Years up to 2022 #####
    if (any(break_years %notin% c(2023,2024))) {
      ### Files names (from ARPA database)
      file_name <- dplyr::case_when(break_years >= 2013 ~ paste0(break_years,".csv"),
                                    break_years %in% 2011:2012 ~ "2012.csv",
                                    break_years %in% 2009:2010 ~ "2010.csv",
                                    break_years %in% 2006:2008 ~ "2008.csv",
                                    break_years %in% 2001:2005 ~ "2005.csv",
                                    break_years %in% 1989:2000 ~ "2000.csv")
      URL_blocks <- as.matrix(paste(URLs,file_name))

      ### Download
      Meteo1 <- do.call(
        rbind,
        future.apply::future_apply(X = as.matrix(URL_blocks[break_years %notin% c(2023,2024),]), MARGIN = 1, FUN = function(x) {

          ######################################
          ########## Downloading data ##########
          ######################################
          link_str <- stringr::str_split(string = x, pattern = " ")[[1]][1]
          file_str <- stringr::str_split(string = x, pattern = " ")[[1]][2]
          options(timeout = 10000)
          Meteo_temp <- readr::read_csv(archive::archive_read(link_str, file = file_str), col_types = readr::cols())
          options(timeout = 100)

          #####################################
          ########## Processing data ##########
          #####################################
          if (verbose == TRUE) {
            cat("Processing data: started at", as.character(Sys.time()), "\n")
          }
          ### Change variable names
          Meteo_temp <- Meteo_temp %>%
            dplyr::select(IDSensor = .data$IdSensore, Date = .data$Data, Value = .data$Valore,
                          Operator = .data$idOperatore) %>%
            dplyr::mutate(Date = lubridate::dmy_hms(.data$Date),
                          IDSensor = as.numeric(.data$IDSensor))
          ### Add metadata
          Meteo_temp <- dplyr::right_join(Meteo_temp,Metadata, by = "IDSensor")
          ### Cleaning
          if (by_sensor %in% c(1,TRUE)) {
            Meteo_temp <- Meteo_temp %>%
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
              dplyr::mutate(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric),
                                          ~ dplyr::na_if(.,-9999))) %>%
              dplyr::mutate(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric),
                                          ~ dplyr::na_if(.,NaN)))
          } else if (by_sensor %in% c(0,FALSE)) {
            Meteo_temp <- Meteo_temp %>%
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
              dplyr::mutate(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric),
                                          ~ dplyr::na_if(.,-9999))) %>%
              dplyr::mutate(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric),
                                          ~ dplyr::na_if(.,NaN)))
          }
          Meteo_temp[is.na(Meteo_temp)] <- NA
          Meteo_temp[is.nan_df(Meteo_temp)] <- NA
          Meteo_temp
        })
      )
    } else {
      Meteo1 <- NULL
    }
    # clean RAM
    invisible(gc())


    #####  Data from 2023 to current (2024) are provided via Socrata API #####
    if (any(break_years %in% c(2023,2024))) {

      ### Filter valid URLs and dates
      URLs_23on <- URLs[break_years %in% c(2023,2024)]
      break_years_23on <- break_years[break_years %in% c(2023,2024)]
      if (lubridate::ymd("2023-01-01") > Date_begin) {
        Date_begin_23on <- lubridate::ymd("2023-01-01")
      } else {
        Date_begin_23on <- Date_begin
      }

      ### Building URLs/links in Socrata format using sequences of dates
      if (length(break_years_23on) == 1) {
        break_dates <- paste0(break_years_23on,"-12-31 23:00:00")
      } else {
        break_dates <- paste0(break_years_23on[-length(break_years_23on)],"-12-31 23:00:00")
      }
      dates_seq_end <- c(Date_begin_23on,lubridate::ymd_hms(break_dates),Date_end)
      dates_seq_end <- unique(dates_seq_end)
      dates_seq_end <- dates_seq_end[-1]
      dates_seq_end <- dates_seq_end[dates_seq_end <= Date_end & dates_seq_end >= Date_begin_23on]
      dates_seq_begin <- c(Date_begin_23on,lubridate::ymd_hms(break_dates) + lubridate::hours(1))
      dates_seq_begin <- unique(dates_seq_begin)
      dates_seq_begin <- dates_seq_begin[dates_seq_begin <= Date_end & dates_seq_begin >= Date_begin_23on]

      URL_blocks <- vector(mode = "list", length = length(dates_seq_begin))
      for (b in 1:length(dates_seq_begin)) {
        seq_temp <- seq(dates_seq_begin[b],
                        dates_seq_end[b],
                        length.out = n_blocks + 1)

        seq_begin_temp <- seq_temp[-length(seq_temp)]
        seq_begin_temp <- lubridate::round_date(seq_begin_temp, unit = "hour")
        seq_begin_temp <- stringr::str_replace(string = seq_begin_temp, pattern = " ", replacement = "T")

        seq_end_temp <- c(seq_temp[-c(1,length(seq_temp))] - lubridate::hours(1),seq_temp[length(seq_temp)])
        seq_end_temp <- lubridate::round_date(seq_end_temp, unit = "hour")
        seq_end_temp <- stringr::str_replace(string = seq_end_temp, pattern = " ", replacement = "T")

        if (is.null(ID_station)) {
          str_sensor <- NULL
        } else {
          str_sensor <- paste0("AND idsensore in(",paste0(sapply(X = Metadata$IDSensor, function(x) paste0("'",x,"'")),collapse = ","),")")
        }

        URL_blocks[[b]] <- data.frame(seq_begin_temp,seq_end_temp) %>%
          dplyr::mutate(link = paste0(URLs_23on[b],"?$where=data between '", seq_begin_temp, "' and '", seq_end_temp,"'", str_sensor)) %>%
          dplyr::select(.data$link)
      }
      URL_blocks <- dplyr::bind_rows(URL_blocks)

      ### Preparing parallel computation: explicitly open multisession/multicore workers by switching plan
      if (parallel == TRUE) {
        future::plan(future::multisession, workers = 12)
        if (is.null(parworkers)) {
          parworkers <- future::availableCores()/2
        }
        eval(parse(text = paste0("future::plan(future::",parfuturetype,", workers = ",parworkers,")")))
        if (verbose == TRUE) {
          message("Start parallel computing: number of parallel workers = ", future::nbrOfWorkers())
        }
      }

      ### Download
      Meteo2 <- do.call(
        rbind,
        future.apply::future_apply(X = as.matrix(URL_blocks), MARGIN = 1, FUN = function(x) {
          RSocrata::read.socrata(url = x, app_token = "Fk8hvoitqvADHECh3wEB26XbO")
        })
      )

      ### Ending parallel computation: explicitly close multisession/multicore workers by switching plan
      if (parallel == TRUE) {
        future::plan(future::sequential)
        if (verbose == TRUE) {
          message("Stop parallel computing: number of parallel workers = ", future::nbrOfWorkers())
        }
      }

      ### Processing data
      if (verbose == TRUE) {
        cat("Processing data: started at", as.character(Sys.time()), "\n")
      }

      ### Change variable names
      Meteo2 <- Meteo2 %>%
        dplyr::select(IDSensor = .data$idsensore, Date = .data$data, Value = .data$valore,
                      Operator = .data$idoperatore) %>%
        dplyr::mutate(IDSensor = as.numeric(.data$IDSensor))

      ### Add metadata
      Meteo2 <- dplyr::right_join(Meteo2,Metadata, by = "IDSensor")

      ### Cleaning
      if (by_sensor %in% c(1,TRUE)) {
        Meteo2 <- Meteo2 %>%
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
          dplyr::mutate(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric),
                                      ~ dplyr::na_if(.,-9999))) %>%
          dplyr::mutate(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric),
                                      ~ dplyr::na_if(.,NaN)))
      } else if (by_sensor %in% c(0,FALSE)) {
        Meteo2 <- Meteo2 %>%
          dplyr::filter(!is.na(.data$Date)) %>%
          dplyr::mutate(Value = as.numeric(.data$Value),
                        Operator = dplyr::case_when(.data$Measure == "Relative_humidity" & .data$Operator == 3 ~ 1,
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
          dplyr::mutate(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric),
                                      ~ dplyr::na_if(.,-9999))) %>%
          dplyr::mutate(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric),
                                      ~ dplyr::na_if(.,NaN)))
      }
      Meteo2[is.na(Meteo2)] <- NA
      Meteo2[is.nan_df(Meteo2)] <- NA
    } else {
      Meteo2 <- NULL
    }



    #####################################
    ########## Processing data ##########
    #####################################

    ### Append datasets
    Meteo <- dplyr::bind_rows(Meteo1,Meteo2)
    rm(Meteo1,Meteo2)
    # clean RAM
    invisible(gc())

    ### Add dataset attributes
    attr(Meteo, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")

    ### Cleaning
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

    if (by_sensor %in% c(0,FALSE)) {
      ### Aggregating dataset
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

      ### Regularizing dataset: same number of timestamps for each station and variable
      if (verbose == TRUE) {
        cat("Regularizing ARPA data: started started at", as.character(Sys.time()), "\n")
      }
      Meteo <- Meteo %>%
        dplyr::arrange(.data$Date) %>%
        dplyr::filter(.data$Date >= Date_begin,
                      .data$Date <= Date_end) %>%
        tidyr::pivot_longer(cols = -c(.data$Date,.data$IDStation,.data$NameStation),
                            names_to = "Measure", values_to = "Value") %>%
        dplyr::mutate(Date = case_when(Frequency %in% c("10mins","hourly") ~ as.character(format(x = .data$Date, format = "%Y-%m-%d %H:%M:%S")),
                                       TRUE ~ as.character(format(x = .data$Date, format = "%Y-%m-%d")))) %>%
        tidyr::pivot_wider(names_from = .data$Date, values_from = .data$Value) %>%
        tidyr::pivot_longer(cols = -c(.data$Measure,.data$IDStation,.data$NameStation),
                            names_to = "Date", values_to = "Value") %>%
        tidyr::pivot_wider(names_from = .data$Measure, values_from = .data$Value)

      if (Frequency %notin% c("10mins","hourly")) {
        Meteo <- Meteo %>%
          dplyr::mutate(Date = ymd(.data$Date)) %>%
          dplyr::arrange(.data$IDStation,.data$Date)
      } else {
        Meteo <- Meteo %>%
          dplyr::mutate(Date = ymd_hms(.data$Date)) %>%
          dplyr::arrange(.data$IDStation,.data$Date)
      }

      freq_unit <- dplyr::case_when(Frequency == "10mins" ~ "10 min",
                                    Frequency == "hourly" ~ "hours",
                                    Frequency == "daily" ~ "days",
                                    Frequency == "weekly" ~ "weeks",
                                    Frequency == "monthly" ~ "months")

      structure(list(Meteo = Meteo))
      attr(Meteo, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
      attr(Meteo, "frequency") <- Frequency
      attr(Meteo, "units") <- freq_unit
    } else if (by_sensor %in% c(1,TRUE)) {
      Meteo <- Meteo %>%
        dplyr::arrange(.data$Date) %>%
        dplyr::filter(.data$Date >= Date_begin,
                      .data$Date <= Date_end)

      structure(list(Meteo = Meteo))
      attr(Meteo, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
      attr(Meteo, "frequency") <- "10mins"
      attr(Meteo, "units") <- "10 min"
    }

    if (verbose == TRUE) {
      cat("Processing data: ended at", as.character(Sys.time()), "\n")
    }

    if (verbose == TRUE) {
      cat("Retrieving desired ARPA Lombardia dataset: ended at", as.character(Sys.time()), "\n")
    }

    return(Meteo)
  }
