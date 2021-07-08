#' @keywords internal
#' @noRd

Custom_summarise <- function(grouped_data,var_vec,fns_vec) {
  # Wind speed and direction can only be averaged
  if(sum((var_vec=="Wind_speed" | var_vec == "Wind_direction") & fns_vec != "mean") > 0) {
    stop("Error: for wind speed and wind direction is possible to calculate only the average value. Use 'mean' in 'Fns_vec.'",
         call. = FALSE)
  }

  # Checks if all the selected variables are available for the actual dataset
  if (all(dplyr::all_of(var_vec) %in% names(grouped_data)) == F) {
    stop("Error: one ore more pollutants are not avaiable for the selected stations! Change the values of 'Var_vec'",
         call. = FALSE)
  }

  summ_data <- grouped_data %>%
    dplyr::summarise(dplyr::across(c(var_vec[(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec == "Wind_speed" & fns_vec == "mean"]), ~ Wind_averaging(Wind_speed,Wind_direction)$Wind_speed,.names = "{.col}_mean"),
                     dplyr::across(c(var_vec[!(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec == "Wind_speed" & fns_vec=="mean"]), ~ Wind_averaging(Wind_speed,Wind_direction)$Wind_speed),
                     dplyr::across(c(var_vec[(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec == "Wind_direction" & fns_vec == "mean"]), ~ Wind_averaging(Wind_speed,Wind_direction)$Wind_direction,.names = "{.col}_mean"),
                     dplyr::across(c(var_vec[!(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec == "Wind_direction" & fns_vec=="mean"]), ~ Wind_averaging(Wind_speed,Wind_direction)$Wind_direction),
                     dplyr::across(c(var_vec[(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & fns_vec == "mean"]), ~ mean(.x, na.rm=T),.names = "{.col}_mean"),
                     dplyr::across(c(var_vec[!(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & fns_vec=="mean"]), ~ mean(.x, na.rm=T)),
                     dplyr::across(c(var_vec[(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & fns_vec=="median"]), ~ median(.x, na.rm=T),.names = "{.col}_median"),
                     dplyr::across(c(var_vec[!(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & (fns_vec=="median")]), ~ median(.x, na.rm=T)),
                     dplyr::across(c(var_vec[(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & grepl('\\bq[0-9]+$',fns_vec)]),
                                     list(!!!quantilep(.data$.x,unique(fns_vec[(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & grepl('\\bq[0-9]+$',fns_vec)]))),
                                   .names = "{.col}_{.fn}"),
                     dplyr::across(c(var_vec[!(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & grepl('\\bq[0-9]+$',fns_vec)]), ~
                                     quantilep(.x,fns_vec[!(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & grepl('\\bq[0-9]+$',fns_vec)])),
                     dplyr::across(c(var_vec[(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & fns_vec == "sum"]), ~ sum(.x, na.rm=T),.names = "{.col}_cum"),
                     dplyr::across(c(var_vec[!(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & fns_vec=="sum"]), ~ sum(.x, na.rm=T)),
                     dplyr::across(c(var_vec[(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & fns_vec == "min"]), ~ min(.x, na.rm=T),.names = "{.col}_min"),
                     dplyr::across(c(var_vec[!(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & fns_vec=="min"]), ~ min(.x, na.rm=T)),
                     dplyr::across(c(var_vec[(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & fns_vec == "max"]), ~ max(.x, na.rm=T),.names = "{.col}_max"),
                     dplyr::across(c(var_vec[!(duplicated(var_vec) | duplicated(var_vec, fromLast = T)) & var_vec != "Wind_speed" & var_vec != "Wind_direction" & fns_vec=="max"]), ~ max(.x, na.rm=T))) %>%
    dplyr::mutate(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric), ~ ifelse(is.infinite(.x), NA, .x)))

  # Drop non-useful columns concerning the quantiles
  if (sum(grepl('\\q[0-9]+$',names(summ_data))) > 0) {
    '%notin%' <- Negate('%in%')
    names_full <- names(summ_data)[grepl('\\q[0-9]+$',names(summ_data))]
    to_drop <- names_full[names_full %notin% paste0(var_vec,"_",fns_vec)]
    summ_data <- summ_data %>%
      dplyr::select(-to_drop)
  }

  return(summ_data)
}

