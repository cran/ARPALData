#' @keywords internal
#' @noRd

W_metadata_reshape <-
  function() {

    ##### Check online availability for weather metadata
    temp <- tempfile()
    res <- curl::curl_fetch_disk("https://www.dati.lombardia.it/resource/nf78-nj6b.csv", temp)
    if(res$status_code != 200) {
      stop(paste0("The internet resource for weather stations metadata is not available at the moment, try later.
                  If the problem persists, please contact the package maintainer."))
    } else {
      Metadata <- RSocrata::read.socrata("https://www.dati.lombardia.it/resource/nf78-nj6b.csv")
    }

    Metadata <- Metadata %>%
      dplyr::rename(IDSensor = .data$idsensore, IDStation = .data$idstazione,
                    Measure = .data$tipologia, NameStation = .data$nomestazione,
                    Altitude = .data$quota, Province = .data$provincia,
                    DateStart = .data$datastart, DateStop = .data$datastop,
                    Latitude = .data$lat, Longitude = .data$lng) %>%
      dplyr::mutate(Altitude = as.numeric(.data$Altitude),
                    DateStart = lubridate::ymd(.data$DateStart),
                    DateStop = lubridate::ymd(.data$DateStop)) %>%
      dplyr::select(.data$IDSensor, .data$IDStation, .data$Measure, .data$NameStation, .data$Altitude,
                    .data$Province, .data$DateStart, .data$DateStop, .data$Latitude, .data$Longitude) %>%
      dplyr::mutate(Measure = dplyr::recode(.data$Measure,
                                            "Altezza Neve" = "Snow_height",
                                            "Direzione Vento" = "Wind_direction",
                                            "Livello Idrometrico" = "Water_height",
                                            "Precipitazione" = "Rainfall",
                                            "Radiazione Globale" = "Global_radiation",
                                            "Temperatura" = "Temperature"),
                    Measure = ifelse(.data$Measure == rlang::as_utf8_character("Umidit\u00e0 Relativa"),"Relative_humidity",.data$Measure),
                    Measure = ifelse(.data$Measure == rlang::as_utf8_character("Velocit\u00e0 Vento"),"Wind_speed",.data$Measure))

    ### Name stations
    Metadata <- Metadata %>%
      dplyr::mutate(dplyr::across(c(.data$NameStation), ~ stringi::stri_trans_general(str = .x, id="Latin-ASCII")),
                    dplyr::across(c(.data$NameStation), toupper),
                    dplyr::across(c(.data$NameStation), ~ gsub("\\-", " ", .x)),
                    dplyr::across(c(.data$NameStation), ~ stringr::str_replace_all(.x, c("S\\."="San ","s\\."="San ",
                                                                                         "V\\."="Via ","v\\."="Via ",
                                                                                         " D\\`" = " D\\' ", " D\\` " = " D\\'",
                                                                                         "D\\`" = " D\\'", "D\\'" = " D\\' "))),
                    dplyr::across(c(.data$NameStation), tm::removePunctuation),
                    dplyr::across(c(.data$NameStation), tm::removeNumbers),
                    dplyr::across(c(.data$NameStation), tm::stripWhitespace),
                    dplyr::across(c(.data$NameStation), stringr::str_to_title),
                    dplyr::across(c(.data$NameStation), ~ stringr::str_replace_all(.x, c(" D " = " D\\'"))))

    return(Metadata)
  }
