# Overall Reserach Question: How do joint errors in 24hr wind and temperature forecasts impact locational marginal prices and renewable curtailment.

# Data Collection:
- hourly 24h weather forecasts from NDFD over Texas. Check code needs to be run to generate all the data but code in download_ndfd should be working.
- hourly realized weather outcomes over Texas. Use following Woerman Market Size and Market Power paper, use hourly weather station measurements from NOAA's integrated surface database. Needs to be pulled our downloaded
    - NOAA api documentation listed here: https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation
- ERCOT hourly LMP and hourly day-ahead clearing price by node or bus. This can be downloaded from ercot's website or potentially pulled using ERCOT's api. 
    - In ~/keys/ I have ercot_api_key.txt ercot_user.txt and ercot_pwd.txt which contain my ercot api and account information.
    - Try to pull the data directly using the api for reproducibility. 
- Map of ERCOT Node name to lat lon coordinate. Allows for spatially analysis of price map
- ERCOT renewable curtailment by hour and renewable generator. I believe is available on ERCOT's website. I beleive is difference between actual output and each units high sustainted output value but i am not sure 
- Shape file of ERCOT nodes and large transmission lines to be used as backbone for possible graph neural network. 