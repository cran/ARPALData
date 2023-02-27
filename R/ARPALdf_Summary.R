#' Summary statistics for a data frame of class 'ARPALdf'
#'
#' @description 'ARPALdf_Summary' returns many descriptive statistics summaring the data contained in a data frame
#' of class ARPALdf. Statistics are calculated at overall level (full sample), by station ID and by year.
#' For each variable are reported the basic positioning indices (min, max, mean, median, quantile) and
#' variability indices (range, standard deviation). Other reported statistics are the linear correlation (Pearson)
#' by station and some graphical representation of the distribution (kernel density plot, histogram,
#' Hampel filter and boxplot). In addition, the function returns useful data-quality information: gap length
#' (i.e. number of missing observations for each variable by station and by year).
#'
#' @param Data Dataset of class 'ARPALdf' containing the data to be summarised.
#' @param by_IDStat Logic value (0 or 1). Use 1 to compute summary statistics by Station ID. Default is 1.
#' @param by_Year Logic value (0 or 1). Use 1 to compute summary statistics by year. Default is 1.
#' @param gap_length Logic value (0 or 1). Use 1 to compute summary statistics for the gap length of each variable.
#' Default is 1.
#' @param correlation Logic value (0 or 1). Use 1 to compute linear correlation of available variables. Default is 1.
#' @param histogram Logic value (0 or 1). Use 1 to plot the histogram of each variable. Default is 0.
#' @param density Logic value (0 or 1). Use 1 to plot the kernel density plot of each variable. Default is 0.
#' @param outlier Logic value (0 or 1). Use 1 to analyse extreme values of each variable
#' (boxplot and Hampel filter). Default is 0.
#' @param verbose Logic value (T or F). Toggle warnings and messages. If 'verbose=T' (default) the function
#' prints on the screen some messages describing the progress of the tasks. If 'verbose=F' any message about
#' the progression is suppressed.
#'
#' @return A list of data.frames containing summary descriptive statistics for a data frame of class 'ARPALdf'.
#' Summary statistics are computed for the overall sample (Descr), by Station ID (Descr_by_IDStat) and by
#' year (Descr_by_Year). Available statistics are: number of NAs, % of NAs over the total sample, number of null values,
#' number of negative values, minimum, mean, maximum and standard deviation.
#'
#' @examples
#' \donttest{
#' ## Download daily air quality data from all the stations for year 2020
#' d <- get_ARPA_Lombardia_AQ_data(ID_station = NULL, Year = 2020, Frequency = "daily")
#' ## Summarising observed data
#' sum_stats <- ARPALdf_Summary(Data = d)
#' }
#'
#' @export

ARPALdf_Summary <- function(Data, by_IDStat = 1, by_Year = 1, gap_length = T, correlation = T,
                            histogram = F, density = F, outlier = F, verbose=T) {

  ### Checks
  stopifnot("histogram must be 1 or 0" = histogram == 0 | histogram == 1)
  stopifnot("density must be 1 or 0" = density == 0 | density == 1)
  stopifnot("by_IDStat must be 1 or 0" = by_IDStat == 0 | by_IDStat == 1)
  stopifnot("by_Year must be 1 or 0" = by_Year == 0 | by_Year == 1)
  stopifnot("gap_length must be 1 or 0" = gap_length == 0 | gap_length == 1)
  stopifnot("Data is not of class 'ARPALdf'" = is_ARPALdf(Data = Data) == T)

  ### Print message about dimensions
  n <- dim(Data)[1]
  m <- length(unique(Data$IDStation))
  t <- length(unique(Data$Date))
  t1 <- min(Data$Date)
  t2 <- max(Data$Date)
  if (verbose==T) {
    cat("The dataset contains: \n")
    cat(paste0("   ** ",n," total observations \n"))
    cat(paste0("   ** ",m," stations/ground sites \n"))
    cat(paste0("   ** ",t," time stamps from ",t1," to ",t2,"\n"))
    cat("Inspect this object to obtain summary statistics: \n")
    cat("   ** on the whole sample (Descr) \n")
    if (by_IDStat == 1) {
      cat("   ** by Station ID (Descr_by_IDStat) \n")
      cat("Attention! NA values of the statistics indicate that data don't exist for a specific Station \n")}
    if (by_Year == 1) {
      cat("   ** by Year (Descr_by_year) \n")
      cat("Attention! NA values of the statistics indicate that data don't exist for a specific Year \n")}
  }

  ### Fix IDStation to integer
  Data <- Data %>%
    dplyr::mutate(IDStation = as.integer(.data$IDStation))


  ### Overall statistics
  NA_count <- Data %>%
    dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                     !tidyselect::vars_select_helpers$where(is.integer), ~ sum(is.na(.x))))
  NA_count_perc <- Data %>%
    dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                     !tidyselect::vars_select_helpers$where(is.integer),
                                   ~ round(sum(is.na(.x))/dim(Data)[1]*100,2)))
  mean_vals <- Data %>%
    dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                     !tidyselect::vars_select_helpers$where(is.integer), ~ round(mean(.x,na.rm=T),2)))
  sd_vals <- Data %>%
    dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                     !tidyselect::vars_select_helpers$where(is.integer), ~ round(sd(.x,na.rm=T),2)))
  min_vals <- Data %>%
    dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                     !tidyselect::vars_select_helpers$where(is.integer), ~ round(min(.x,na.rm=T),2)))
  max_vals <- Data %>%
    dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                     !tidyselect::vars_select_helpers$where(is.integer), ~ round(max(.x,na.rm=T),2)))
  null_vals <- Data %>%
    dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                     !tidyselect::vars_select_helpers$where(is.integer), ~ sum(.x == 0, na.rm=T)))
  neg_vals <- Data %>%
    dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                     !tidyselect::vars_select_helpers$where(is.integer), ~ sum(.x < 0, na.rm=T)))

  descriptives <- dplyr::bind_rows(NA_count,NA_count_perc,null_vals,neg_vals,min_vals,mean_vals,max_vals,sd_vals)
  descriptives <- t(descriptives)
  descriptives <- data.frame(descriptives) %>%
    tibble::rownames_to_column()
  colnames(descriptives) <- c("Var","NA_count","NA_perc","Null_count","Negative_count",
                              "Min","Mean","Max","Std.Dev.")
  if (is_ARPALdf_AQ(Data = Data) == T) {
    attr(descriptives, "class") <- c("ARPALdf","ARPALdf_AQ","tbl_df","tbl","data.frame")
  } else if (is_ARPALdf_W(Data = Data) == T) {
    attr(descriptives, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
  }


  ### Statistics by Station ID
  if (by_IDStat == 1) {
    if (verbose==T) {
      cat("Computing summary statistics by Station ID \n")
    }
    NA_count_stat <- Data %>%
      dplyr::group_by(.data$IDStation) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ sum(is.na(.x))))
    NA_count_perc_stat <- Data %>%
      dplyr::group_by(.data$IDStation) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer),
                                     ~ round(sum(is.na(.x))/dplyr::n()*100,2)))
    mean_vals_stat <- Data %>%
      dplyr::group_by(.data$IDStation) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ round(mean(.x, na.rm=T),2)))
    sd_vals_stat <- Data %>%
      dplyr::group_by(.data$IDStation) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ round(sd(.x, na.rm=T),2)))
    min_vals_stat <- Data %>%
      dplyr::group_by(.data$IDStation) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ round(min(.x,na.rm=T),2)))
    max_vals_stat <- Data %>%
      dplyr::group_by(.data$IDStation) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ round(max(.x,na.rm=T),2)))
    null_vals_stat <- Data %>%
      dplyr::group_by(.data$IDStation) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ sum(.x == 0, na.rm=T)))
    neg_vals_stat <- Data %>%
      dplyr::group_by(.data$IDStation) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ sum(.x < 0, na.rm=T)))
    Descr_by_IDStat <- list(NA_count_stat,NA_count_perc_stat,null_vals_stat,neg_vals_stat,
                            min_vals_stat,mean_vals_stat,max_vals_stat,sd_vals_stat)
    names(Descr_by_IDStat) <- c("NA_count_by_stat","NA_perc_by_stat","Null_count_by_stat","Negative_count_by_stat",
                                "Min_by_stat","Mean_by_stat","Max_by_stat","StdDev_by_stat")
    for (i in 1:length(Descr_by_IDStat)) {
      Descr_by_IDStat[[i]][is.nan_df(Descr_by_IDStat[[i]])] <- NA
      Descr_by_IDStat[[i]][is.infinite_df(Descr_by_IDStat[[i]])] <- NA
      if (is_ARPALdf_AQ(Data = Data) == T) {
        attr(Descr_by_IDStat[[i]], "class") <- c("ARPALdf","ARPALdf_AQ","tbl_df","tbl","data.frame")
      } else if (is_ARPALdf_W(Data = Data) == T) {
        attr(Descr_by_IDStat[[i]], "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
      }
    }
  }


  ### Statistics by Year
  if (by_Year == 1) {
    if (verbose==T) {
      cat("Computing summary statistics by Year \n")
    }
    NA_count_year <- Data %>%
      dplyr::group_by(lubridate::year(.data$Date)) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ sum(is.na(.x))))
    NA_count_perc_year <- Data %>%
      dplyr::group_by(lubridate::year(.data$Date)) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer),
                                     ~ round(sum(is.na(.x))/dplyr::n()*100,2)))
    mean_vals_year <- Data %>%
      dplyr::group_by(lubridate::year(.data$Date)) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ round(mean(.x, na.rm=T),2)))
    sd_vals_year <- Data %>%
      dplyr::group_by(lubridate::year(.data$Date)) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ round(sd(.x, na.rm=T),2)))
    min_vals_year <- Data %>%
      dplyr::group_by(lubridate::year(.data$Date)) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ round(min(.x,na.rm=T),2)))
    max_vals_year <- Data %>%
      dplyr::group_by(lubridate::year(.data$Date)) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ round(max(.x,na.rm=T),2)))
    null_vals_year <- Data %>%
      dplyr::group_by(lubridate::year(.data$Date)) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ sum(.x == 0, na.rm=T)))
    neg_vals_year <- Data %>%
      dplyr::group_by(lubridate::year(.data$Date)) %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ sum(.x < 0, na.rm=T)))
    Descr_by_year <- list(NA_count_year,NA_count_perc_year,null_vals_year,neg_vals_year,
                          min_vals_year, mean_vals_year,max_vals_year,sd_vals_year)
    names(Descr_by_year) <- c("NA_count_by_year","NA_perc_by_year","Null_count_by_year","Negative_count_by_year",
                              "Min_by_year","Mean_by_year","Max_by_year","StdDev_by_year")
    for (i in 1:length(Descr_by_year)) {
      Descr_by_year[[i]][is.nan_df(Descr_by_year[[i]])] <- NA
      Descr_by_year[[i]][is.infinite_df(Descr_by_year[[i]])] <- NA
      if (is_ARPALdf_AQ(Data = Data) == T) {
        attr(Descr_by_year[[i]], "class") <- c("ARPALdf","ARPALdf_AQ","tbl_df","tbl","data.frame")
      } else if (is_ARPALdf_W(Data = Data) == T) {
        attr(Descr_by_year[[i]], "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
      }
    }
  }


  ### Gap length by variable
  if (gap_length == 1) {
    if (verbose==T) {
      cat("Computing gap lengths statistics by variable \n")
    }
    vars <- Data %>%
      dplyr::select(tidyselect::vars_select_helpers$where(is.double) &
                      tidyselect::vars_select_helpers$where(is.numeric) &
                      !tidyselect::vars_select_helpers$where(is.integer)) %>%
      colnames()
    gap_length <- vector("list", length = length(vars))
    for (v in 1:length(vars)) {
      var <- vars[v]
      gl <- Data %>%
        dplyr::select(.data$Date,.data$IDStation,.data$NameStation,var = var) %>%
        dplyr::filter(!is.na(.data$var)) %>%
        dplyr::group_by(.data$IDStation,.data$NameStation) %>%
        dplyr::summarise(.groups = "keep",
                         gap = lubridate::interval(.data$Date,.data$Date[-1])) %>%
        dplyr::mutate(gap = lubridate::time_length(.data$gap,unit = attributes(Data)$units)) %>%
        dplyr::filter(.data$gap > 0) %>%
        dplyr::summarise(.groups = "keep",
                         min_gap = min(.data$gap),
                         q25_gap = quantile(.data$gap,probs = 0.25),
                         mean_gap = round(mean(.data$gap),3),
                         median_gap = quantile(.data$gap,probs = 0.50),
                         q75_gap = quantile(.data$gap,probs = 0.75),
                         max_gap = max(.data$gap),
                         sd_gap_length = round(sd(.data$gap),3),
                         # Va sistemato per versione CRAN
                         Length1 = sum(.data$gap == 1),
                         Length2 = sum(.data$gap == 2),
                         Length24 = sum(.data$gap == 24)) %>%
        as.data.frame() %>%
        dplyr::mutate(dplyr::across(tidyselect::contains("gap"),
                                    ~ mondate::as.difftime(.x,units = attributes(Data)$units)))
      colnames(gl) <- c("IDStation","NameStation",
                        paste0(var,"_min_gap"),paste0(var,"_q25_gap"),
                        paste0(var,"_mean_gap"),paste0(var,"_median_gap"),
                        paste0(var,"_q75_gap"),paste0(var,"_max_gap"),paste0(var,"_sd_gap"),
                        paste0(var,"_freq_gap1"),paste0(var,"_freq_gap2"),paste0(var,"_freq_gap24"))
      if (is_ARPALdf_AQ(Data = Data) == T) {
        attr(gl, "class") <- c("ARPALdf","ARPALdf_AQ","tbl_df","tbl","data.frame")
      } else if (is_ARPALdf_W(Data = Data) == T) {
        attr(gl, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
      }
      gap_length[[v]] <- gl
    }
    names(gap_length) <- vars
  }


  ### Histogram
  if (histogram == 1) {
    if (verbose==T) {
      cat("Graphics: plotting histogram of each variable \n")
    }
    hist_plot <- Data %>%
      dplyr::select(tidyselect::vars_select_helpers$where(is.numeric) &
                      !tidyselect::vars_select_helpers$where(is.integer)) %>%
      tidyr::pivot_longer(cols = dplyr::everything()) %>%
      ggplot2::ggplot(aes(.data$value)) +
      ggplot2::geom_histogram(bins = 30, fill="blue") +
      ggplot2::facet_wrap(~ .data$name, scales = "free")
    print(hist_plot)
  }


  ### Kernel density plot
  if (density == 1) {
    if (verbose==T) {
      cat("Graphics: plotting density of each variable \n")
    }
    dens_plot <- Data %>%
      dplyr::select(tidyselect::vars_select_helpers$where(is.numeric) &
                      !tidyselect::vars_select_helpers$where(is.integer)) %>%
      tidyr::pivot_longer(cols = dplyr::everything()) %>%
      ggplot2::ggplot(aes(.data$value)) +
      ggplot2::geom_density(fill="blue",alpha = 0.5) +
      ggplot2::facet_wrap(~ .data$name, scales = "free")
    print(dens_plot)
  }


  ### Outlier analysis
  if (outlier == 1) {
    if (verbose==T) {
      cat("Computing outlier statisics for each variable \n")
    }
    Data_long <- Data %>%
      tidyr::pivot_longer(cols = tidyselect::vars_select_helpers$where(is.numeric) &
                            !tidyselect::vars_select_helpers$where(is.integer),
                          names_to = "Vars", values_to = "Value")
    boxp <- ggplot(Data_long, aes(y = .data$Value)) +
      geom_boxplot(aes(fill = .data$Vars)) +
      coord_flip() +
      facet_wrap(~ .data$Vars, scales = "free") +
      labs(title = "Boxplot on the whole sample by variable") +
      theme(legend.position = "")
    print(boxp)
    if (verbose==T) {
      cat("Computing Hampel filter for each variable \n")
      cat("Reports the % of observations above +3*MAD and below -3*MAD \n")
    }
    out <- Data %>%
      dplyr::summarise(dplyr::across(tidyselect::vars_select_helpers$where(is.numeric) &
                                       !tidyselect::vars_select_helpers$where(is.integer), ~ Hampel_flt(.x))) %>%
      list()
    hampel <- matrix(NA,nrow = dim(out[[1]])[2], ncol = 7)
    for (i in 1:dim(out[[1]])[2]) {
      hampel[i,2] <- round(as.numeric(out[[1]][[i]]$I_low),2)
      hampel[i,3] <- round(length(out[[1]][[i]]$outlier_ind_low),2)
      hampel[i,4] <- round(length(out[[1]][[i]]$outlier_ind_low)/dim(Data)[1]*100,2)
      hampel[i,5] <- round(as.numeric(out[[1]][[i]]$I_upp),2)
      hampel[i,6] <- round(length(out[[1]][[i]]$outlier_ind_upp),2)
      hampel[i,7] <- round(length(out[[1]][[i]]$outlier_ind_upp)/dim(Data)[1]*100,2)
    }
    hampel <- data.frame(hampel)
    hampel[,1] <- names(out[[1]])
    colnames(hampel) <- c("Variable","Lower_bound","Obs_below_low_count","Obs_below_low_perc","Upper_bound","Obs_above_upp_count","Obs_above_upp_perc")
    if (is_ARPALdf_AQ(Data = Data) == T) {
      attr(hampel, "class") <- c("ARPALdf","ARPALdf_AQ","tbl_df","tbl","data.frame")
    } else if (is_ARPALdf_W(Data = Data) == T) {
      attr(hampel, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
    }
  }


  ### Correlation analysis
  if (correlation == 1) {
    if (verbose==T) {
      cat("Computing linear correlation analysis for available variable \n")
    }
    stz <- unique(Data$IDStation)
    name_stz <- unique(Data$NameStation)
    cor_matrix <- vector("list", length = length(stz))
    for (s in 1:length(stz)) {
      cor_matrix[[s]] <- Data %>%
        dplyr::filter(.data$IDStation == stz[s]) %>%
        dplyr::select(tidyselect::vars_select_helpers$where(is.double) &
                        tidyselect::vars_select_helpers$where(is.numeric)) %>%
        stats::cor(use = "pairwise.complete.obs") %>%
        as.data.frame() %>%
        tibble::rownames_to_column(var = "Var1") %>%
        tidyr::pivot_longer(cols = -.data$Var1, names_to = "Var2", values_to = "corr") %>%
        dplyr::mutate(IDStation = stz[s], NameStation = name_stz[s]) %>%
        dplyr::filter(.data$Var1 != .data$Var2) %>%
        tidyr::pivot_wider(names_from = c("Var1","Var2"),values_from = "corr")
    }
    cor_matrix <- bind_rows(cor_matrix)
    if (is_ARPALdf_AQ(Data = Data) == T) {
      attr(cor_matrix, "class") <- c("ARPALdf","ARPALdf_AQ","tbl_df","tbl","data.frame")
    } else if (is_ARPALdf_W(Data = Data) == T) {
      attr(cor_matrix, "class") <- c("ARPALdf","ARPALdf_W","tbl_df","tbl","data.frame")
    }
  }


  ### Output list
  ret_list <- list(Descr = descriptives)
  if (exists("Descr_by_IDStat", inherits = F)) {
    ret_list <- c(ret_list, Descr_by_IDStat = list(Descr_by_IDStat))
  }
  if (exists("Descr_by_year", inherits = F)) {
    ret_list <- c(ret_list, Descr_by_year = list(Descr_by_year))
  }
  if (exists("hampel", inherits = F)) {
    ret_list <- c(ret_list, Hampel = list(hampel))
  }
  if (exists("gap_length", inherits = F)) {
    ret_list <- c(ret_list, Gap_length = list(gap_length))
  }
  if (exists("cor_matrix", inherits = F)) {
    ret_list <- c(ret_list, Cor_matrix = list(cor_matrix))
  }


  ### Output
  return(ret_list)
}


