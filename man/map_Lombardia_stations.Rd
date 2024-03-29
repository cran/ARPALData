% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/map_Lombardia_stations.R
\name{map_Lombardia_stations}
\alias{map_Lombardia_stations}
\title{Generate a map of the selected stations}
\usage{
map_Lombardia_stations(
  data,
  title = "Map of ARPA stations in Lombardy",
  prov_line_type = 1,
  prov_line_size = 1,
  col_points = "blue",
  xlab = "Longitude",
  ylab = "Latitude"
)
}
\arguments{
\item{data}{Dataset of class 'ARPALdf' containing the stations to plot on the map. It can be either a
ARPALdf of observed data (from 'get_ARPA_Lombardia_xxx' commands) and an ARPALdf obtained as registry
(from 'get_ARPA_Lombardia_xxx_registry' command).}

\item{title}{Title of the plot. Deafult is 'Map of ARPA stations in Lombardy'}

\item{prov_line_type}{Linetype for Lombardy provinces. Default is 1.}

\item{prov_line_size}{Size of the line for Lombardy provinces. Default is 1.}

\item{col_points}{Color of the points. Default is 'blue'.}

\item{xlab}{x-axis label. Default is 'Longitude'.}

\item{ylab}{y-axis label. Default is 'Latitude'.}
}
\value{
A map of selected stations across the Lombardy region
}
\description{
'get_ARPA_Lombardia_AQ_data' represents on a map (geometries/polygon of Lombardy) the location of
the stations contained in a data frame of class 'ARPALdf'. Data can be either a ARPALdf of observed data
(from 'get_ARPA_Lombardia_xxx' commands) and an ARPALdf obtained as registry
(from 'get_ARPA_Lombardia_xxx_registry' command).
}
\examples{
\donttest{
## Map network from a dataset of measurements
if (require("RSocrata")) {
  # Download daily concentrations observed at all the stations in 2020.
  d <- get_ARPA_Lombardia_AQ_data(ID_station = NULL, Date_begin = "2020-01-01",
                                  Date_end = "2020-12-31", Frequency = "daily")
  # Map the stations included in 'd'
  map_Lombardia_stations(data = d, title = "Air quality stations in Lombardy")
}
## Map network from a registry dataset
if (require("RSocrata")) {
  # Download registry for all the AQ stations in 2020.
  r <- get_ARPA_Lombardia_AQ_registry()
  # Map the stations included in 'r'
  map_Lombardia_stations(data = r, title = "Air quality stations in Lombardy")
}
}

}
