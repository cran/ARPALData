#' @keywords internal
#' @noRd

W_metadata_reshape <-
  function() {
    Metadata <- RSocrata::read.socrata("https://www.dati.lombardia.it/resource/nf78-nj6b.csv")
    Metadata <- Metadata %>%
      dplyr::rename(IDSensor = .data$idsensore, IDStation = .data$idstazione,
                    Measure = .data$tipologia, NameStation = .data$nomestazione,
                    Altitude = .data$quota, Province = .data$provincia,
                    DateStart = .data$datastart, DateStop = .data$datastop,
                    Latitude = .data$lat, Longitude = .data$lng) %>%
      dplyr::mutate(Altitude = as.numeric(.data$Altitude),
                    DateStart = lubridate::ymd(.data$DateStart),
                    DateStop = lubridate::ymd(.data$DateStop),
                    Measure = stringi::stri_escape_unicode(.data$Measure)) %>%
      dplyr::select(.data$IDSensor, .data$IDStation, .data$Measure, .data$NameStation, .data$Altitude,
                    .data$Province, .data$DateStart, .data$DateStop, .data$Latitude, .data$Longitude) %>%
      dplyr::mutate(Measure = dplyr::recode(.data$Measure,
                                            "Altezza Neve" = "Snow_height",
                                            "Direzione Vento" = "Wind_direction",
                                            "Livello Idrometrico" = "Water_height",
                                            "Precipitazione" = "Rainfall",
                                            "Radiazione Globale" = "Global_radiation",
                                            "Temperatura" = "Temperature",
                                            "Umidit\\u00e0 Relativa" = "Relative_humidity",
                                            "Velocit\\u00e0 Vento" = "Wind_speed"))

    ### Name stations
    Metadata <- Metadata %>%
      dplyr::mutate(dplyr::across(c(.data$NameStation), ~ stringi::stri_trans_general(str = .x, id="Latin-ASCII")),
                    dplyr::across(c(.data$NameStation), toupper),
                    dplyr::across(c(.data$NameStation), ~ gsub("\\-", " ", .x)),
                    dplyr::across(c(.data$NameStation), ~ stringr::str_replace_all(.x, c("S\\."="San ","s\\."="San ",
                                                                                         "V\\."="Via ","v\\."="Via "))),
                    dplyr::across(c(.data$NameStation), tm::removePunctuation),
                    dplyr::across(c(.data$NameStation), tm::removeNumbers),
                    dplyr::across(c(.data$NameStation), tm::stripWhitespace),
                    dplyr::across(c(.data$NameStation), stringr::str_to_title))

    return(Metadata)
  }
