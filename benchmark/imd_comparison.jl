using CSV, Dates, BenchmarkTools, InMemoryDatasets
using Revise
using AbnormalReturns

using LinearAlgebra
using Random
##
# ds_firm = filereader(joinpath("data", "firm_ret.csv"), types = Dict(2=>Date)) 
ds_firm = CSV.File(joinpath("data", "firm_ret.csv")) |> Dataset # 100870000 rows
ds_mkt = CSV.File(joinpath("data", "mkt_ret.csv")) |> Dataset  # 10087
ds_events = CSV.File(joinpath("data", "event_dates.csv")) |> Dataset |> unique # 996384




##
@time ds_all = innerjoin(
    ds_firm,
    ds_mkt,
    on=[:date]
);
# run with 4 threads:  12.779321 seconds (19.32 M allocations: 8.492 GiB, 8.64% gc time, 48.57% compilation time)
# run with 6 threads: 11.598963 seconds (19.09 M allocations: 8.480 GiB, 9.01% gc time, 54.17% compilation time)
# second run with 6 threads: 6.939663 seconds (569 allocations: 7.422 GiB, 10.22% gc time)


##


@time ds_event_joined = innerjoin(
    ds_all,
    ds_events[:, [:firm_id, :event_date, :est_window_start, :est_window_end]],
    on=[:firm_id => :firm_id, :date => (:est_window_start, :est_window_end)]
)
# 138.604128 seconds (16.02 M allocations: 24.040 GiB, 10.80% gc time, 7.26% compilation time)
# run with 4 threads: 80.307892 seconds (15.99 M allocations: 24.040 GiB, 7.34% gc time, 8.49% compilation time)
# run with 6 threads: 77.349717 seconds (15.99 M allocations: 24.040 GiB, 18.15% gc time, 9.95% compilation time)
# second run with 6 threads:  63.473872 seconds (11.32 k allocations: 23.182 GiB, 20.56% gc time)

##
@time groupby!(ds_event_joined, [:firm_id, :event_date])
#  90.437869 seconds (1.36 M allocations: 26.803 GiB, 22.00% gc time, 1.15% compilation time)
# run with 4 threads: 72.804367 seconds (1.36 M allocations: 26.804 GiB, 15.97% gc time, 0.90% compilation time)
# run with 6 threads: 68.599091 seconds (1.36 M allocations: 26.804 GiB, 19.86% gc time, 1.79% compilation time)
# second run with 6 threads: 0.001346 seconds (19 allocations: 944 bytes)

##
function simple_reg(xs...)
    pred = disallowmissing(hcat(xs[2:end]...))
    resp = disallowmissing(xs[1])

    [cholesky!(Symmetric(pred' * pred)) \ (pred' * resp)]
end;



@time InMemoryDatasets.combine(ds_event_joined, (:ret, :mkt, :hml, :umd, :smb) => simple_reg)
#  38.805655 seconds (45.03 M allocations: 20.162 GiB, 14.69% gc time, 16.15% compilation time)
# run with 4 threads: 26.969340 seconds (44.98 M allocations: 20.161 GiB, 16.68% gc time, 16.81% compilation time)
# run with 6 threads: 21.928671 seconds (45.26 M allocations: 20.176 GiB, 18.18% gc time, 18.37% compilation time)
# second run with 6 threads: 16.797414 seconds (34.87 M allocations: 19.629 GiB, 16.77% gc time)
##

# @time ds_event_joined = innerjoin(
#     ds_all,
#     ds_events[:, [:firm_id, :event_date, :event_window_start, :event_window_end]],
#     on=[:firm_id => :firm_id, :date => (:event_window_start, :event_window_end)]
# )
# #  39.920199 seconds (11.09 k allocations: 8.180 GiB, 12.29% gc time)

# ##

# @time groupby!(ds_event_joined, [:firm_id, :event_date])
#   5.214217 seconds (10.64 k allocations: 2.320 GiB, 43.41% gc time)

# Random.seed!(1234)
# function splitdf(df, pct)
#     @assert 0 <= pct <= 1
#     ids = collect(axes(df, 1))
#     shuffle!(ids)
#     sel = ids .<= nrow(df) .* pct
#     return view(df, sel, :)
# end;

# ds_firm=splitdf(ds_firm,0.1);
# ds_mkt=splitdf(ds_mkt,0.1);
# ds_events=splitdf(ds_events,0.1);

