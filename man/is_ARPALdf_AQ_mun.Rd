% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/is_ARPALdf_AQ_mun.R
\name{is_ARPALdf_AQ_mun}
\alias{is_ARPALdf_AQ_mun}
\title{Check if a given object belongs to class 'ARPALdf_AQ_mun'}
\usage{
is_ARPALdf_AQ_mun(Data)
}
\arguments{
\item{Data}{Object to check if the class of a dataframe is 'ARPALdf_AQ_mun', i.e. ARPAL
dataframe for air quality data at municipal level (See 'get_ARPA_Lombardia_AQ_municipal_data'.
command).}
}
\value{
The function returns 'True' if the object is of class 'ARPALdf_AQ_mun' and it returns
'False' if the object isn't of class 'ARPALdf_AQ_mun'
}
\description{
'is_ARPALdf_AQ_mun' checks if the input object belongs to the class 'ARPALdf_AQ_mun'
}
\examples{
d <- get_ARPA_Lombardia_AQ_registry()
is_ARPALdf_AQ_mun(d)

}
