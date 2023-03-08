# MDEStudy (Market Data Event Study)
---
## Dataset Introduction
We need three data sets as input, one for firm returns (firm_ret), one for market returns(mkt_ret), and one for event dates(event_dates).

First is the company return, in firm_ret, we have firm id(firm_id), firm return date(date), return (ret).
The second is market return. In mkt_ret, there are market return value(mkt), market return date(date), and some optional factors, such as smb,hml,umb, etc.
The third is the firm's event date. In event_dates, We have the firm id(firm_id), the date the event started (event_window_start), the date the event ended (event_window_end), the start date without the event (est_window_start) and the end date without the event (est_window_start).


## Generating the dataset
The example dataset is simulated data. If you want to use the sample data for testing, follow these steps:
Find create_data.jl in MDES/benchmark/ and run it, this process will take a while.
```
include("create_data.jl")
```
Make sure that there is a folder named "data" in the MDES/benchmark/ directory where the dataset files will be placed. And the data is in millions, so make sure you have enough storage.
In total, three datasets are generated: ds_firm, ds_mkt, and ds_events.


## Loading dataset
```
using DLMreader
using Dates
```
DLMreader is an efficient multi-threaded package for reading(writing) delimited files. It works very well for huge files.
The date is read in as a string. We need to convert it to a date by using Dates.
```
ds_firm = filereader(joinpath("data", "firm_ret.csv"), types = Dict(2=>Date)); 
ds_mkt = filereader(joinpath("data", "mkt_ret.csv"), types = Dict(1=>Date));
ds_events=filereader(joinpath("data","event_dates.csv"),types = Dict(2:6 .=>Date)) |> unique;
```

## Combine datasets
```
data = MarketData(ds_mkt, ds_firm; id_col=:firm_id, valuecols_firms=[:ret])
```
leftJoin ds_firm with ds_mkt by "date" column. In this process, we will take the date in ds_mkt as businessday to check whether the date in ds_firm is businessday, and if there is any discrepancy, we will throw an error.
*MarketData* will return a MarketData struct.
Note: All datasets will be sorted by date inside the MarketData function.

"MarketData()" has three fields
    calendar
    firmdata
    marketdata
 The businessday result is stored in "calendar" field.
 "firmdata" is the leftjoin  result of sorted ds_firm.
 "marketdata" is the sorted ds_mkt without "date" column.


## Calculate the abnormal return

We can then use the events data set ds_events, the curated data set data, and the user-provided model (represented as a formula) as arguments to the group_and_reg function.
```
result=group_and_reg(ds_events,data, @formula(ret ~ mkt + smb + hml + umd))
```
Formula format: @formula(response_name ~ factor_names)
Users can customize the formula to fit different expected return models.

The result is an AbResult struct which has fields:
    formula: the user's input formula
    coef: a matrix of coefficient of the model
    expected: expected return
    actual: actual return
    abr: abnormal return
    xnames:a vector of factor names and intercept of model
    yname : response variable name.



    






















