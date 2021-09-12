#' @keywords internal
#' @noRd

url_dataset_year <-
  function(Stat_type, Year) {
    url <- switch(Stat_type,
                  W = dplyr::case_when(Year == 2021 ~ "https://www.dati.lombardia.it/api/odata/v4/647i-nhxk",
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

                  AQ = dplyr::case_when(Year == 2021 ~ "https://www.dati.lombardia.it/api/odata/v4/nicp-bhqi",
                                        Year == 2020 ~ "https://www.dati.lombardia.it/download/88sp-5tmj/application%2Fzip",
                                        Year == 2019 ~ "https://www.dati.lombardia.it/api/views/j2mz-aium/files/f3d03850-4750-4168-a3a7-fdfb204bee14?filename=sensori_aria_2019.zip",
                                        Year == 2018 ~ "https://www.dati.lombardia.it/api/views/4t9j-fd8z/files/01b6ae2f-85a8-4cf2-a558-d9c5ffa2e5e8?filename=sensori_aria_2018.zip",
                                        Year == 2017 ~ "https://www.dati.lombardia.it/api/views/fdv6-2rbs/files/742fb7a8-2a58-4b08-a366-a75c358be1ed?filename=sensori_aria_2017.zip",
                                        Year == 2016 ~ "https://www.dati.lombardia.it/api/views/7v3n-37f3/files/3b4f1e13-0e42-48bd-bfcc-ddc173a2c3d5?filename=sensori_aria_2016.zip",
                                        Year == 2015 ~ "https://www.dati.lombardia.it/api/views/bpin-c7k8/files/e06bd244-03f6-4b19-9095-bed347eaf9cb?filename=sensori_aria_2015.zip",
                                        Year == 2014 ~ "https://www.dati.lombardia.it/api/views/69yc-isbh/files/8d2f96b2-a5dc-441b-b791-9c82390d7f43?filename=sensori_aria_2014.zip",
                                        Year == 2013 ~ "https://www.dati.lombardia.it/api/views/hsdm-3yhd/files/53344428-e917-4bb4-b6d2-24141b600f44?filename=sensori_aria_2013.zip",
                                        Year == 2012 ~ "https://www.dati.lombardia.it/api/views/wr4y-c9ti/files/d6c78bb2-4fd1-4931-9b21-71e04bc6885e?filename=sensori_aria_2012.zip",
                                        Year == 2011 ~ "https://www.dati.lombardia.it/api/views/5mut-i45n/files/aef76013-6b23-41fd-940d-02b08f27f2fd?filename=sensori_aria_2011.zip",
                                        Year %in% 2008:2010 ~ "https://www.dati.lombardia.it/download/wp2f-5nw6/application%2Fzip",
                                        Year %in% 2005:2007 ~ "https://www.dati.lombardia.it/download/h3i4-wm93/application%2Fzip",
                                        Year %in% 2001:2004 ~ "https://www.dati.lombardia.it/download/5jdj-7x8y/application%2Fzip",
                                        Year %in% 1996:2000 ~ "https://www.dati.lombardia.it/download/wabv-jucw/application%2Fzip"),

                  AQ_municipal = dplyr::case_when(Year == 2021 ~ "https://www.dati.lombardia.it/download/56c9-hxta/application%2Fzip",
                                                  Year == 2020 ~ "https://www.dati.lombardia.it/download/ej5v-5krk/application%2Fzip",
                                                  Year == 2019 ~ "https://www.dati.lombardia.it/download/dupr-g65c/application%2Fzip",
                                                  Year == 2018 ~ "https://www.dati.lombardia.it/download/v75z-59qh/application%2Fzip",
                                                  Year == 2017 ~ "https://www.dati.lombardia.it/download/a7tn-gnv9/application%2Fzip"))

    return(url)
  }

