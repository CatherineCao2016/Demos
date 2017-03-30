# This R script is to request historical daily weather data from Weather Underground API. 
#
# Register and get an API key here: https://www.wunderground.com/weather/api/
#
# Updated: March 29, 2017 by Catherine.Cao@ibm.com

#########################################################

#Note: To run this script, you will need to

# a. Register and get an API key at https://www.wunderground.com/weather/api/
# b. set up API key in set_api_key("Insert API Key")
# c. Use dashDB provided or insert your dashDB credentials 
# d. If you are using your own dashDB instance, make sure you have table HISTORY_CLAIM_CLEANED in your dashDB
# e. Specify start date and end date in df <- get_daily_weather(171, "YYYY-MM-DD", "YYYY-MM-DD"). 
#    For this solution demo, the date range is from 2015-01-01 to 2015-12-31. 
#    It may take a long time to load weather data for one year because the API call is limited at 10 calls per minute and 500 calls per day for free trial account. 
#    So we recommend you to use the weather data("weather_data_cleaned") provided for this specific demo. 
#    Request for a more advanced account if you need more resources.
# f. After you finish the steps above, select all the code and click Run to run the script.

#########################################################

# setup: load required packages and set api key

# function to check existence before install packages
packages <- function(x){
  x <- as.character(match.call()[[2]])
  if (!require(x,character.only=TRUE)){
    install.packages(pkgs=x,repos="http://cran.r-project.org")
    require(x,character.only=TRUE)
  }
}

packages(ibmdbR)
packages(dplyr)

# install package rwunderground
packages(devtools)
install_github("CatherineCao2016/rwunderground")
library(rwunderground)

# set api key
set_api_key("Insert API Key")

# Connect to dashDB: Insert dashDB credentials

dsn_driver <- "BLUDB" 
dsn_database <- "BLUDB"  
dsn_hostname <-  "dashdb-entry-yp-dal09-10.services.dal.bluemix.net"  
dsn_port <- "50000"  
dsn_protocol <- "TCPIP"  
dsn_uid <- "dash13990"
dsn_pwd <- "a8ae96f334fb"

conn_path <- paste(dsn_driver,  
                   ";DATABASE=",dsn_database,
                   ";HOSTNAME=",dsn_hostname,
                   ";PORT=",dsn_port,
                   ";PROTOCOL=",dsn_protocol,
                   ";UID=",dsn_uid,
                   ";PWD=",dsn_pwd,sep="")

con <- idaConnect(conn_path)  
idaInit(con)  

# Get location informatoin from the historiy claim data

claim_loc <- idadf(con, "SELECT LATITUDE, LONGITUDE, CUSTOMER_LOCATION_ID FROM HISTORY_CLAIM_CLEANED")

loc171 <- unique(claim_loc[c("LATITUDE", "LONGITUDE", "CUSTOMER_LOCATION_ID")])

loc171$latlong <- paste0(loc171$LATITUDE, ",", loc171$LONGITUDE)

# function to request weather data for the 171 locations
# n: number of locations
# startdate, enddate: the input format should be "YYYY-MM-DD"

get_daily_weather <- function(n, startdate, enddate){
  
  callsMade = 0
  weather_daily <- data.frame()
  
  i = 1
  
  while (i <= n) {
    
    start_date = as.Date(startdate)
    datestr <- strsplit(as.character(start_date), "-")
    datestr <- paste0(datestr[[1]][1], datestr[[1]][2], datestr[[1]][3])
    
    while (start_date <= enddate){
      
      daily <- history_daily(set_location(loc171[,4][i]), datestr)
      daily$CUSTOMER_LOCATION_ID <- loc171[,3][i]
      
      callsMade = callsMade + 1
      
      if (callsMade == 10){
        Sys.sleep(60)
      }
      
      weather_daily <- rbind(weather_daily, daily)
      
      start_date = start_date + 1
      
      datestr <- strsplit(as.character(start_date), "-")
      datestr <- paste0(datestr[[1]][1], datestr[[1]][2], datestr[[1]][3])
    }
    
    cat("\n", i, "of 171 completed", "\n")
    
    i = i+1
    callsMade = 0
  }
  
  weather_daily <- as.data.frame(weather_daily)
  return(weather_daily)
  
}

# request historical weather data: in the solution demo, the date range is from 2015-01-01 to 2015-12-31.
#eg.get daily weather data for the first two locations for the first two weeks of 2015
#df <- get_daily_weather(2, "2015-01-01", "2015-01-14")
df <- get_daily_weather(171, "YYYY-MM-DD", "YYYY-MM-DD")

# clean data
# keep variables needed
df_sub <- df[c("date", "snow_fall", "mean_temp", "mean_wind_spd", "precip", "CUSTOMER_LOCATION_ID")]

# delete NAs.
df_sub_noNA <- na.omit(df_sub)

# get week number
df_sub_noNA$CAL_WEEK_OF_YEAR <- as.numeric(as.Date(df_sub_noNA$date) - as.Date("2015-01-01")) %/% 7 + 1

df_sub_noNA$CAL_WEEK_OF_YEAR <- ifelse(as.Date(df_sub_noNA$date) > as.Date("2015-12-23") & as.Date(df_sub_noNA$date) < as.Date("2016-01-01"), 52, df_sub_noNA$CAL_WEEK_OF_YEAR)

# summarize by week
grouped <- group_by(df_sub_noNA, CUSTOMER_LOCATION_ID, CAL_WEEK_OF_YEAR)
weather_summary <- summarise(grouped, 
                             mean_temp = round(mean(mean_temp),2),
                             mean_wind = round(mean(mean_wind_spd),2),
                             mean_snow = round(mean(snow_fall),2),
                             mean_precip = round(mean(precip),2))

summary(weather_summary)

# save to dashDB: it will create a table named WeatherDatafromAPI in dashDB
idaSave(con, weather_summary, tblName = "WeatherDatafromAPI", rowName = "", conType = "odbc")

