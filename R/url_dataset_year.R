#' @keywords internal
#' @noRd

url_dataset_year <-
  function(Stat_type, Year) {
    url <- switch(Stat_type,
                  ##### Weather monitoring network
                  W = dplyr::case_when(Year %in% 2023:2024 ~ "https://www.dati.lombardia.it/resource/647i-nhxk.json",
                                       Year == 2022 ~ "https://www.dati.lombardia.it/download/mvvc-nmzv/application%2Fzip",
                                       Year == 2021 ~ "https://www.dati.lombardia.it/download/49n9-866s/application%2Fzip",
                                       Year == 2020 ~ "https://www.dati.lombardia.it/download/erjn-istm/application%2Fzip",
                                       Year == 2019 ~ "https://www.dati.lombardia.it/download/wrhf-6ztd/application%2Fzip",
                                       Year == 2018 ~ "https://www.dati.lombardia.it/download/sfbe-yqe8/application%2Fzip",
                                       Year == 2017 ~ "https://www.dati.lombardia.it/download/vx6g-atiu/application%2Fzip",
                                       Year == 2016 ~ "https://www.dati.lombardia.it/download/kgxu-frcw/application%2Fzip",
                                       Year == 2015 ~ "https://www.dati.lombardia.it/download/knr4-9ujq/application%2Fzip",
                                       Year == 2014 ~ "https://www.dati.lombardia.it/download/fn7i-6whe/application%2Fzip",
                                       Year == 2013 ~ "https://www.dati.lombardia.it/download/76wm-spny/application%2Fzip",
                                       Year %in% 2011:2012 ~ "https://www.dati.lombardia.it/download/srpn-ykcs/application%2Fzip",
                                       Year %in% 2009:2010 ~ "https://www.dati.lombardia.it/download/9nu5-ed8s/application%2Fzip",
                                       Year %in% 2006:2008 ~ "https://www.dati.lombardia.it/download/6udq-c5ub/application%2Fzip",
                                       Year %in% 2001:2005 ~ "https://www.dati.lombardia.it/download/stys-ktts/application%2Fzip",
                                       Year %in% 1989:2000 ~ "https://www.dati.lombardia.it/download/tj2h-b7vd/application%2Fzip"),

                  ##### Web pages of the weather data
                  W_check = dplyr::case_when(Year %in% 2023:2024 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo/647i-nhxk",
                                             Year == 2022 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2022/mvvc-nmzv",
                                             Year == 2021 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2021/49n9-866s",
                                             Year == 2020 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2020/erjn-istm",
                                             Year == 2019 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2019/wrhf-6ztd",
                                             Year == 2018 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2018/sfbe-yqe8",
                                             Year == 2017 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2017/vx6g-atiu",
                                             Year == 2016 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2016/kgxu-frcw",
                                             Year == 2015 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2015/knr4-9ujq",
                                             Year == 2014 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2014/fn7i-6whe",
                                             Year == 2013 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2013/76wm-spny",
                                             Year %in% 2011:2012 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2011-2012/srpn-ykcs",
                                             Year %in% 2009:2010 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2009-2010/9nu5-ed8s",
                                             Year %in% 2006:2008 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2006-2008/6udq-c5ub",
                                             Year %in% 2001:2005 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2001-2005/stys-ktts",
                                             Year %in% 1989:2000 ~ "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-1989-2000/tj2h-b7vd"),

                  ##### Air quality from ground network
                  AQ = dplyr::case_when(Year %in% 2023:2024 ~ "https://www.dati.lombardia.it/resource/nicp-bhqi.json",
                                        Year %in% 2018:2022 ~ "https://www.dati.lombardia.it/resource/g2hp-ar79.json",
                                        Year %in% 2010:2017 ~ "https://www.dati.lombardia.it/resource/nr8w-tj77.json",
                                        Year %in% 2000:2009 ~ "https://www.dati.lombardia.it/resource/cthp-zqrr.json",
                                        Year %in% 1968:1999 ~ "https://www.dati.lombardia.it/resource/evzn-32bs.json"),

                  ##### Municipal data
                  AQ_municipal = dplyr::case_when(Year %in% 2023:2024 ~ "https://www.dati.lombardia.it/resource/ysm5-jwrn.json",
                                                  Year == 2022 ~ "https://www.dati.lombardia.it/resource/fqaz-7ste.json",
                                                  Year == 2021 ~ "https://www.dati.lombardia.it/download/56c9-hxta/application%2Fzip",
                                                  Year == 2020 ~ "https://www.dati.lombardia.it/download/ej5v-5krk/application%2Fzip",
                                                  Year == 2019 ~ "https://www.dati.lombardia.it/download/dupr-g65c/application%2Fzip",
                                                  Year == 2018 ~ "https://www.dati.lombardia.it/download/v75z-59qh/application%2Fzip",
                                                  Year == 2017 ~ "https://www.dati.lombardia.it/download/a7tn-gnv9/application%2Fzip",
                                                  Year %in% 2011:2016 ~ "https://www.dati.lombardia.it/download/yjvq-g3tp/application%2Fzip"),

                  ##### Web pages of the municipal data
                  AQ_municipal_check = dplyr::case_when(Year %in% 2023:2024 ~ "https://www.dati.lombardia.it/Ambiente/Dati-stime-comunali/ysm5-jwrn",
                                                        Year == 2022 ~ "https://www.dati.lombardia.it/Ambiente/Dati-stime-comunali-2022/fqaz-7ste",
                                                        Year == 2021 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-Comunali-2021/56c9-hxta",
                                                        Year == 2020 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-Comunali-2020/ej5v-5krk",
                                                        Year == 2019 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-Comunali-2019/dupr-g65c",
                                                        Year == 2018 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-Comunali-2018/v75z-59qh",
                                                        Year == 2017 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-Comunali-2017/a7tn-gnv9",
                                                        Year %in% 2011:2016 ~ "https://www.dati.lombardia.it/Ambiente/Dati-Stime-Comunali-2011-2016/yjvq-g3tp")
    )

    return(url)
  }

