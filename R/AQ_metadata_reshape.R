AQ_metadata_reshape <-
function() {
  Metadata <- RSocrata::read.socrata("https://www.dati.lombardia.it/resource/ib47-atvt.csv")
  Metadata <- Metadata %>%
    dplyr::rename(IDSensor = .data$idsensore, IDStation = .data$idstazione,
                  Pollutant = .data$nometiposensore, NameStation = .data$nomestazione,
                  Altitude = .data$quota, Province = .data$provincia, City = .data$comune,
                  DateStart = .data$datastart, DateStop = .data$datastop,
                  Latitude = .data$lat, Longitude = .data$lng) %>%
    dplyr::mutate(Altitude = as.numeric(.data$Altitude),
                  DateStart = lubridate::ymd(.data$DateStart),
                  DateStop = lubridate::ymd(.data$DateStop)) %>%
    dplyr::select(.data$IDSensor, .data$IDStation, .data$Pollutant, .data$NameStation, .data$Altitude,
                  .data$Province, .data$City, .data$DateStart, .data$DateStop, .data$Latitude, .data$Longitude) %>%
    dplyr::mutate(Pollutant = dplyr::recode(.data$Pollutant,
                                            "Ammoniaca" = "Ammonia",
                                            "Arsenico" = "Arsenic",
                                            "Benzene" = "Benzene",
                                            "Benzo(a)pirene" = "Benzo_a_pyrene",
                                            "Biossido di Azoto" = "NO2",
                                            "Monossido di Azoto" = "NO",
                                            "Ossidi di Azoto" = "NOx",
                                            "Biossido di Zolfo" = "Sulfur_dioxide",
                                            "BlackCarbon" = "BlackCarbon",
                                            "Monossido di Carbonio" = "CO",
                                            "Nikel" = "Nikel",
                                            "Ozono" = "Ozone",
                                            "Cadmio" = "Cadmium",
                                            "PM10 (SM2005)" = "PM10",
                                            "PM10" = "PM10",
                                            "Particelle sospese PM2.5" = "PM2.5",
                                            "Particolato Totale Sospeso" = "PM_tot",
                                            "Piombo" = "Lead"))

  ### Add extra information from ARPA offices (uploaded on Paolo Maranzano's GitHub page)
  # ARPA_zone = ARPA Lombardia zoning of the region: https://www.arpalombardia.it/Pages/Aria/Rete-di-rilevamento/Zonizzazione.aspx
  # ARPA_stat_type = stations type: https://www.arpalombardia.it/Pages/Aria/Rete-di-rilevamento/Criteri-di-rilevamento/Tipologia-delle-stazioni.aspx?firstlevel=Ieri
  Metadata_ARPA_url <- "https://raw.githubusercontent.com/PaoloMaranzano/ARPALData/main/AQ_stations_ARPA_Lombardia.csv"
  Metadata_ARPA <- readr::read_csv(Metadata_ARPA_url)
  Metadata_ARPA <- Metadata_ARPA %>%
    dplyr::select(.data$IDStation,.data$ARPA_zone,ARPA_stat_type = .data$Type)
  Metadata <- dplyr::left_join(Metadata,Metadata_ARPA,by = c("IDStation"))


  structure(list(Metadata = Metadata))
  attr(Metadata, "class") <- c("ARPALdf","tbl_df","tbl","data.frame")
  return(Metadata)
}
