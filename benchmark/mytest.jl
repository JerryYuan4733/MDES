using Dates, BenchmarkTools, InMemoryDatasets
using Revise
using AbnormalReturns
using DLMReader
using LinearAlgebra, Random
using Unrolled



ds_firm = filereader(joinpath("data", "firm_ret.csv"), types = Dict(2=>Date)) 
ds_mkt = filereader(joinpath("data", "mkt_ret.csv"), types = Dict(1=>Date));
ds_events=filereader(joinpath("data","event_dates.csv"),types = Dict(2:6 .=>Date)) |> unique;


@time IMD.leftjoin!(ds_firm, ds_mkt[!, [:date]], on = :date, obs_id = [false, true])
# 9.554551 seconds (18.55 M allocations: 3.019 GiB, 5.20% gc time, 59.21% compilation time)
# 1 threads second run: 2.472221 seconds (81.86 k allocations: 1.508 GiB, 9.02% gc time, 1.41% compilation time)
# 4 threads:  7.670734 seconds (24.06 M allocations: 3.323 GiB, 7.50% gc time, 83.31% compilation time)
# 4 threads second run: 1.172847 seconds (81.88 k allocations: 1.508 GiB, 20.61% gc time, 3.67% compilation time)
# 6 threads: 7.470740 seconds (24.06 M allocations: 3.323 GiB, 7.71% gc time, 85.09% compilation time)
# 6 threads second run: 0.978876 seconds (81.89 k allocations: 1.508 GiB, 17.10% gc time, 4.15% compilation time)


m = disallowmissing(ds_mkt[!, 2:5]|>Matrix)




# Two are allocated, one sorted by firm_id and one sorted by date
# before sorted: Base.summarysize(ds_firm)/1024/1024
# 2597.323984146118
# after sorted: Base.summarysize(ds_firm)/1024/1024
# 4617.463748931885
@time IMD.sort!(ds_firm,[:firm_id,:date])
# 6.751503 seconds (5.02 M allocations: 4.689 GiB, 8.78% gc time, 31.84% compilation time)
@time IMD.sort!(ds_mkt,:date)
# 0.240302 seconds (334.07 k allocations: 18.401 MiB, 98.87% compilation time)

@time obs_ids=Int64[]
# 0.000002 seconds (1 allocation: 64 bytes)
""" 
this function, for each row of ds_firm,  find the index of the corresponding row in ds_mkt
"""
@unroll function  map_mkt_data_to_firm_data(dates::AbstractVector,V,ds::AbstractVector)

    @unroll for d in dates
         push!(V,searchsortedfirst(ds,d))
     end
 end



@time map_mkt_data_to_firm_data(IMD._columns(ds_firm)[2],obs_ids,IMD._columns(ds_mkt)[1])
# 5.358733 seconds (64.61 k allocations: 5.328 MiB, 0.85% compilation time)



function simple_reg(obs_id,y; m = m
    )::Vector{Float64}

    pred = view(m,obs_id,:)
    resp = y
    cholesky!(Symmetric(pred' * pred)) \ (pred' * resp)
end;



# @time re_output=Vector{Float64}[]
# 0.000002 seconds (1 allocation: 64 bytes)
@time re_output=zeros(Float64, nrow(ds_events), 4)

""" 
for each line of ds_events,  find the corresponding range in ds_firm,
and do the regression
"""
# function group_and_reg(id,the_start,the_end)
function group_and_reg(ds_e,ds_f,re)
    for n in 1:nrow(ds_e)

        # i=searchsorted(IMD._columns(ds_f)[1],view(ds_e,n,:firm_id)[1])#39M
        i=searchsorted(IMD._columns(ds_f)[1],ds_e[n,:firm_id]) 


        e=searchsortedlast(view(IMD._columns(ds_f)[2],i), IMD._columns(ds_e)[6][n])
        s=searchsortedfirst(view(IMD._columns(ds_f)[2],i), IMD._columns(ds_e)[5][n])
        obs_id=view(IMD._columns(ds_f)[4],i[1]+s-1:i[1]+e-1)

        # obs_id=view(obs_ids,i[1]+s-1:i[1]+e-1)
        # ret=IMD._columns(view(ds_f,i[1]+s-1:i[1]+e-1,3:3))[1]
        ret=view(IMD._columns(ds_f)[3],i[1]+s-1:i[1]+e-1)
  
        re[n,:]=simple_reg(obs_id,ret)
        # e=searchsortedlast(view(IMD._columns(ds_f)[2],i), view(ds_e,n,:est_window_end)[1])
        # s=searchsortedfirst(view(IMD._columns(ds_f)[2],i), view(ds_e,n,:est_window_start)[1])

        # e=searchsortedlast(view(IMD._columns(ds_f)[2],i), ds_e[n,:est_window_end])
        # s=searchsortedfirst(view(IMD._columns(ds_f)[2],i), ds_e[n,:est_window_start])

        # e=searchsortedlast(view(IMD._columns(ds_f)[2],i), view(IMD._columns(ds_e)[6],n)[1])
        # s=searchsortedfirst(view(IMD._columns(ds_f)[2],i), view(IMD._columns(ds_e)[5],n)[1])



        # e=searchsortedlast(IMD._columns(ds_f)[2][i], IMD._columns(ds_e)[6][n])
        # s=searchsortedfirst(IMD._columns(ds_f)[2][i], IMD._columns(ds_e)[5][n])

    end

end;


@time group_and_reg(ds_events,ds_firm,re_output)
# 14.453152 seconds (22.91 M allocations: 915.196 MiB, 0.89% gc time, 0.30% compilation time)

@time for i in 1:2
    group_and_reg(ds_events[i,:firm_id],ds_events[i,:est_window_start],ds_events[i,:est_window_end])
end
# 15.176856 seconds (56.67 M allocations: 2.606 GiB, 4.88% gc time, 0.04% compilation time)


@time byrow(ds_events,group_and_reg,(:firm_id,:est_window_start,:est_window_end))
# 14.136302 seconds (47.27 M allocations: 2.496 GiB, 6.28% gc time, 1.32% compilation time)









@time ds_event_joined=IMD.innerjoin(
    ds_firm,
    view(ds_events,:,[:firm_id,:event_date,:est_window_start, :est_window_end]), 
    on=[:firm_id => :firm_id, :date => (:est_window_start, :est_window_end)],
    );
# 115.654700 seconds (14.67 M allocations: 17.010 GiB, 6.81% gc time, 6.50% compilation time)
# run with 4 threads:  58.524648 seconds (7.51 M allocations: 16.613 GiB, 12.74% gc time, 6.83% compilation time)
# run with 6 threads: 32.617801 seconds (14.67 M allocations: 17.010 GiB, 6.06% gc time, 17.52% compilation time)
# second run with 6 threads: 35.282279 seconds (11.14 k allocations: 16.205 GiB, 11.54% gc time)





@time groupby!(ds_event_joined, [:firm_id, :event_date],stable=false)
#  79.080849 seconds (2.73 M allocations: 19.911 GiB, 4.84% gc time, 1.64% compilation time)
# run with 6 threads: 35.726548 seconds (1.64 M allocations: 19.862 GiB, 9.33% gc time, 5.06% compilation time)


@time j=IMD.combine(ds_event_joined, ( :obs_id_right,:ret) => simple_reg => :re_output )
# 17.010435 seconds (30.98 M allocations: 3.499 GiB, 9.47% gc time, 22.09% compilation time)
# run with 4 threads:  10.071312 seconds (14.77 M allocations: 965.885 MiB, 6.66% gc time, 15.58% compilation time)
# run with 6 threads: 8.359918 seconds (19.96 M allocations: 1.222 GiB, 11.31% gc time, 47.56% compilation time)
# second run with 6 threads: 3.187782 seconds (9.96 M allocations: 741.199 MiB, 10.31% gc time)




