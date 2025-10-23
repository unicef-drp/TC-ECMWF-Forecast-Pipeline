[![TC Forecast Pipeline](https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline/actions/workflows/ecmwf-tc-pipline.yml/badge.svg?branch=main)](https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline/actions/workflows/ecmwf-tc-pipline.yml)

Preview: [https://ahead-of-the-storm.onrender.com](https://ahead-of-the-storm.onrender.com)

### Pipeline schedule
ECMWF issues new forecasts at **00, 06, 12, and 18 UTC**, but the data is typically not published until around **07:41, 11:40, 19:41, and 23:40 UTC**.  

To align with these publication times, the pipeline is scheduled to run at:  

- **09:00 UTC**  
- **13:00 UTC**  
- **21:00 UTC**
- **01:00 UTC**   

This ensures the forecasts are available before the pipeline starts. 
