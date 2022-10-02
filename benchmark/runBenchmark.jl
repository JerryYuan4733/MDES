using Revise
using Pkg
Pkg.develop("AbnormalReturnPkg")
using Dates
using DLMReader
using AbnormalReturnPkg


ds_firm = filereader(joinpath("data", "firm_ret.csv"), types = Dict(2=>Date)); 
ds_mkt = filereader(joinpath("data", "mkt_ret.csv"), types = Dict(1=>Date));
ds_events=filereader(joinpath("data","event_dates.csv"),types = Dict(2:6 .=>Date));


@time data = MarketData(ds_mkt, ds_firm; id_col=:firm_id, valuecols_firms=[:ret])
# 3.448509 seconds (50.13 k allocations: 2.727 GiB, 4.79% gc time, 0.17% compilation time)

ds_firm=nothing
ds_mkt=nothing
GC.gc()

@time coef,er,ar=group_and_reg(ds_events,data, @formula(ret ~ mkt + smb + hml + umd))
# 5.003406 seconds (31.92 M allocations: 2.003 GiB, 8.02% gc time)

abr=er-ar

@time coef,er,ar=group_and_reg(ds_events,data, @formula(ret ~ mkt))
# 3.176684 seconds (31.92 M allocations: 1.816 GiB, 5.92% gc time)