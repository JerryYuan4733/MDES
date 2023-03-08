using Revise
using Pkg
Pkg.develop("MDES")
using Dates
using DLMReader
using MDEStudy
using InMemoryDatasets

ds_firm = filereader(joinpath("data", "firm_ret.csv"), types = Dict(2=>Date)); 
ds_mkt = filereader(joinpath("data", "mkt_ret.csv"), types = Dict(1=>Date));
ds_events=filereader(joinpath("data","event_dates.csv"),types = Dict(2:6 .=>Date)) |> unique;


@time data = MarketData(ds_mkt, ds_firm; id_col=:firm_id, valuecols_firms=[:ret])
# 3.448509 seconds (50.13 k allocations: 2.727 GiB, 4.79% gc time, 0.17% compilation time)

ds_firm=nothing
ds_mkt=nothing
GC.gc()
@time r=group_and_reg(ds_events,data, @formula(ret ~ mkt + smb + hml + umd))
# 5.003406 seconds (31.92 M allocations: 2.003 GiB, 8.02% gc time)

# 5.328198 seconds (9.97 M allocations: 1.426 GiB, 4.99% gc time)


@time r2=group_and_reg(ds_events,data, @formula(ret ~ mkt))
# 3.176684 seconds (31.92 M allocations: 1.816 GiB, 5.92% gc time)

# 3.263101 seconds (9.97 M allocations: 1.160 GiB, 3.39% gc time)