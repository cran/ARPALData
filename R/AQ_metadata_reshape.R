#' @keywords internal
#' @noRd

AQ_metadata_reshape <-
  function() {

    '%notin%' <- Negate('%in%')

    ##### Check online availability for AQ metadata
    temp <- tempfile()
    res <- suppressWarnings(try(curl::curl_fetch_disk("https://www.dati.lombardia.it/resource/ib47-atvt.csv", temp), silent = TRUE))
    if(res$status_code != 200) {
      message(paste0("The internet resource for air quality stations metadata is not available at the moment. Status code: ",res$status_code,".\nPlease, try later. If the problem persists, please contact the package maintainer."))
      return(invisible(NULL))
    } else {
      ### Modified on 2026-06-15: replace socratadata::soc_read with the internal JSON/SODA reader.
      Metadata <- ARPAL_read_socrata_json(url = "https://www.dati.lombardia.it/resource/ib47-atvt.json")
    }

    Metadata <- Metadata %>%
      dplyr::rename(IDSensor = .data$idsensore, IDStation = .data$idstazione,
                    Pollutant = .data$nometiposensore, NameStation = .data$nomestazione,
                    Altitude = .data$quota, Province = .data$provincia, City = .data$comune,
                    DateStart = .data$datastart, DateStop = .data$datastop,
                    Latitude = .data$lat, Longitude = .data$lng) %>%
      dplyr::mutate(
                    ### Modified on 2026-06-15: parse Socrata JSON date/datetime strings robustly.
                    DateStart = ARPAL_parse_socrata_date(.data$DateStart),
                    DateStop = ARPAL_parse_socrata_date(.data$DateStop),
                    IDStation = as.numeric(.data$IDStation),
                    IDSensor = as.numeric(.data$IDSensor)) %>%
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

    ### Name stations
    # dplyr::across(c(.data$NameStation,.data$City), ~ stringi::stri_trans_general(str = .x, id="Latin-ASCII"))
    Metadata <- Metadata %>%
      dplyr::mutate(dplyr::across(c(.data$NameStation,.data$City), toupper),
                    dplyr::across(c(.data$NameStation,.data$City), ~ gsub("\\-", " ", .x)),
                    dplyr::across(c(.data$NameStation,.data$City), ~ stringr::str_replace_all(.x, c("S\\."="San ",
                                                                                                    "V\\."="Via ",
                                                                                                    "V\\.LE" = "Viale",
                                                                                                    " D\\`" = " D\\' ",
                                                                                                    " D\\` " = " D\\'",
                                                                                                    "D\\`" = " D\\'",
                                                                                                    "D\\'" = " D\\' ",
                                                                                                    "P\\.ZZA" = "Piazza",
                                                                                                    "C\\.SO" = "Corso",
                                                                                                    "LOC\\." = "Localita"))),
                    dplyr::across(c(.data$NameStation,.data$City), tm::removePunctuation),
                    dplyr::across(c(.data$NameStation,.data$City), tm::removeNumbers),
                    dplyr::across(c(.data$NameStation,.data$City), tm::stripWhitespace),
                    dplyr::across(c(.data$NameStation,.data$City), stringr::str_to_title),
                    dplyr::across(c(.data$NameStation,.data$City), ~ stringr::str_replace_all(.x, c(" D " = " D\\'",
                                                                                                    " Xi " = " XI ",
                                                                                                    " Xxv " = " XXV ",
                                                                                                    " Via " = " - Via ",
                                                                                                    " Corso " = " - Corso ",
                                                                                                    " Localita " = rlang::as_utf8_character(" - Localit\u00e0 "),
                                                                                                    " Piazza " = " - Piazza ",
                                                                                                    " Via Le " = " Viale ",
                                                                                                    "F Turati" = "Turati",
                                                                                                    "Via Serafino Delluomo" = "Via Serafino Dell'Uomo",
                                                                                                    "Via Dellartigianato" = "Via Dell'Artigianato",
                                                                                                    "Montanaso Lombardo Sp" = "Montanaso Lombardo SP202",
                                                                                                    "Sp Casa Dellalpino" = " - SP27 Casa Dell'Alpino",
                                                                                                    "Spino D'Adda Sp" = "Spino D'Adda SP1",
                                                                                                    "Villasanta - Via A Volta" = "Villasanta - Via Volta",
                                                                                                    "Vimercate - Via Dellospedale" = "Vimercate - Via Dell'Ospedale",
                                                                                                    "Ss Sempione" = "SS Sempione"))))


    ### Add extra information from ARPA offices (uploaded on Paolo Maranzano's GitHub page)
    # ARPA_zone = ARPA Lombardia zoning of the region: https://www.arpalombardia.it/Pages/Aria/Rete-di-rilevamento/Zonizzazione.aspx
    # ARPA_stat_type = stations type: https://www.arpalombardia.it/Pages/Aria/Rete-di-rilevamento/Criteri-di-rilevamento/Tipologia-delle-stazioni.aspx?firstlevel=Ieri

    ##### Read further AQ metadata bundled with the package
    ### Modified on 2026-06-26: use the package-bundled AQ stations metadata
    ### stored in inst/extdata, instead of checking and downloading the same
    ### file from the ARPALData GitHub repository at runtime.
    Metadata_ARPA_file <- ARPAL_extdata_path("AQ_stations_ARPA_Lombardia.csv")
    Metadata_ARPA <- readr::read_csv(Metadata_ARPA_file, show_col_types = FALSE)
    Metadata_ARPA <- Metadata_ARPA %>%
      dplyr::select(.data$IDStation,.data$ARPA_zone,.data$ARPA_stat_type) %>%
      dplyr::distinct()
    Metadata <- dplyr::left_join(Metadata,Metadata_ARPA,by = c("IDStation"))
    Metadata <- Metadata %>%
      filter(.data$IDStation %notin% c(518,602,603,612,694,698,700))
    # Galliate 518 (NO) --> Fuori regione, chiusa nel 2017
    # Melegnano 602 --> Chiusa dal 2017 --> Chiusa dal 2017
    # Filago via Fermi Marne (612) --> Chiusa dal 2017
    # Castiraga 603 --> Chiusa dal 2017
    # Salionze 694 (VR) --> Fuori regione
    # Ceneselli 698 (RO) --> Fuori regione
    # Melara 700 (RO) --> Fuori regione, chiusa dal 2018


    structure(list(Metadata = Metadata))
    attr(Metadata, "class") <- c("ARPALdf","tbl_df","tbl","data.frame")
    return(Metadata)
  }
