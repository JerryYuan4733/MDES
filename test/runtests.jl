using Test
using Dates
using DLMReader
using MDEStudy
using InMemoryDatasets

# cd MDEStudy/test

## one factor model
# 0.000209 seconds (246 allocations: 23.609 KiB)
ds_firm = filereader(joinpath("data","firm1.csv"), types = Dict(1=>Date)); 
ds_mkt = filereader(joinpath( "data","mkt1.csv"), types = Dict(1=>Date));
ds_events=filereader(joinpath("data","event1.2.csv"),types = Dict(2:6 .=>Date));


@time data = MarketData(ds_mkt, ds_firm; id_col=:firm_id, valuecols_firms=[:ret])

# [0.044, -0.015, 0.009, 0.015, -0.026, -0.01, -0.005]

@time r=group_and_reg(ds_events,data, @formula(ret ~ mkt))
#  resp-(intercept+beta*mkt(predictor))
#  -0.0027-(0.00046772+1.28*-0.0157)



## million rows observations
ds_events=filereader(joinpath("data","e_big.csv"),types = Dict(2:6 .=>Date));

@time r=group_and_reg(ds_events,data, @formula(ret ~ mkt))
