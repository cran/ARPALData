#' @keywords internal
#' @noRd

url_AQ_year_dataset <-
  function(Stat_type, Year) {
    url <- switch(Stat_type,
                  ##### Air quality from ground network
                  ### Modified on 2026-06-15: use the current-year endpoint for
                  ### Dati sensori aria and the consolidated historical endpoint for
                  ### Dati sensori aria dal 2018.
                  AQ = dplyr::case_when(Year >= 2026 ~ "https://www.dati.lombardia.it/resource/nicp-bhqi.json",
                                        Year %in% 2018:2025 ~ "https://www.dati.lombardia.it/resource/g2hp-ar79.json",
                                        Year %in% 2010:2017 ~ "https://www.dati.lombardia.it/resource/nr8w-tj77.json",
                                        Year %in% 2000:2009 ~ "https://www.dati.lombardia.it/resource/cthp-zqrr.json",
                                        Year %in% 1968:1999 ~ "https://www.dati.lombardia.it/resource/evzn-32bs.json"),

                  ##### Municipal data
                  AQ_municipal = dplyr::case_when(Year >= 2025 ~ "https://www.dati.lombardia.it/resource/ysm5-jwrn.json",
                                                  Year == 2024 ~ "https://www.dati.lombardia.it/resource/qyg8-q6gd.json",
                                                  Year == 2023 ~ "https://www.dati.lombardia.it/resource/q25y-843e.json",
                                                  Year == 2022 ~ "https://www.dati.lombardia.it/resource/fqaz-7ste.json",
                                                  Year == 2021 ~ "https://www.dati.lombardia.it/resource/7iq4-hq9t.json",
                                                  Year %in% 2017:2020 ~ "https://www.dati.lombardia.it/resource/aip6-75fk.json",
                                                  ### Modified on 2026-06-15: replace the older zipped municipal archive with the
                                                  ### current Socrata JSON endpoint for Dati Stime Comunali 2011-2016.
                                                  Year %in% 2011:2016 ~ "https://www.dati.lombardia.it/resource/miv5-s38a.json"),

                  ##### Web pages of the municipal data
                  ### Modified on 2026-06-15: align municipal-data check URLs with
                  ### the current Open Data Lombardia ARPA catalogue/story page.
                  ### In particular, 2024 and 2023 have year-specific pages, 2021
                  ### uses dataset id 7iq4-hq9t, and 2017--2020 are exposed as a
                  ### single consolidated dataset.
                  AQ_municipal_check = dplyr::case_when(Year >= 2025 ~ "https://www.dati.lombardia.it/Ambiente/Dati-stime-comunali/ysm5-jwrn",
                                                        Year == 2024 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-Comunali-2024/qyg8-q6gd",
                                                        Year == 2023 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-comunali-2023/q25y-843e",
                                                        Year == 2022 ~ "https://www.dati.lombardia.it/Ambiente/Dati-stime-comunali-2022/fqaz-7ste",
                                                        Year == 2021 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-Comunali-2021/7iq4-hq9t",
                                                        Year %in% 2017:2020 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-Comunali-2017-2020/aip6-75fk",
                                                        ### Modified on 2026-06-15: update the 2011-2016 municipal check page to
                                                        ### the current Open Data Lombardia dataset id miv5-s38a.
                                                        Year %in% 2011:2016 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-Comunali-2011-2016/miv5-s38a")
    )

    return(url)
  }

