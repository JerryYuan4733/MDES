using DLMReader, InMemoryDatasets
using DataFrames, DataFramesMeta, Dates, BenchmarkTools, Cthulhu
using AbnormalReturnPkg

ds_firm = filereader(joinpath("data", "firm_ret.csv"), types = Dict(2=>Date)) 
ds_mkt = filereader(joinpath("data", "mkt_ret.csv"), types = Dict(1=>Date));
ds_events=filereader(joinpath("data","event_dates.csv"),types = Dict(2:6 .=>Date)) |> unique;


