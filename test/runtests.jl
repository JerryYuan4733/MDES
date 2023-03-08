using Dates
using DLMReader
using MEDS
using Test
using InMemoryDatasets


ds_firm = filereader(joinpath("data", "daily_ret.csv"), types = Dict(2=>Date)) 
ds_mkt = filereader(joinpath("data", "mkt_ret.csv"), types = Dict(1=>Date)) 
ds_res = filereader(joinpath("data", "car_results.csv"), types = Dict(2 =>Date) )
ds_mkt[!, :mkt] = ds_mkt.mktrf .+ ds_mkt.rf

rename!(ds_firm,:permno=>:firm_id)

@test data = MarketData(ds_mkt, ds_firm)


# d=Dataset([:firm_id=>[18428,18429],:event_date=>[Date(2005-07-25),Date(2006-07-25)],
# :event_window_start=>[Date(2005-07-11),Date(2005-07-11)],:event_window_end=>
# [Date(2005-08-08),Date(2005-08-08)],:est_window_start=>[Date(2019-04-01),Date(2019-04-01)],
# :est_window_end=>[Date(2019-10-01),Date(2019-10-01)]])

# @time r=group_and_reg(d, data, @formula(ret ~ mktrf + hml))




