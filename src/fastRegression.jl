"""
    struct AbResults
        formula::FormulaTerm
        coef::Matrix{Float64}
        expected::Vector{Float64}
        actual::Vector{Float64}
        abr::Vector{Float64}
        xnames
        yname
    end

# Arguments
- `formula`:A StatsModels.jl formula, saved in the resulting struct
- `coef`: A matrix which stores the coefficients of final model
- `expected`: A vector which stores the expected return
- `actual`:A vector which stores the actual return
- `abr`: A vector which stores the abnormal return (actual return - expected return)
- `xnames`: the factor names of formular, the names are from column names of market data 
- `yname`: the response names of formular, the name is from column name of firm data

# Example
```@setup general
using DLMReader
using MDEStudy
ds_firm = filereader(joinpath(pathof(MDEStudy),"..","..","benchmark","data", "firm_ret.csv"), types = Dict(2=>Date)); 
ds_mkt = filereader(joinpath(pathof(MDEStudy),"..","..","benchmark","data", "mkt_ret.csv"), types = Dict(1=>Date));
ds_events=filereader(joinpath(pathof(MDEStudy),"..","..","benchmark","data","event_dates.csv"),types = Dict(2:6 .=>Date)) |> unique;
mkt_data = MarketData(
ds_mkt,
ds_firm
)
reg_result=group_and_reg(ds_events,data, @formula(ret ~ mkt + smb + hml + umd))
typeof(reg_result)
```
"""
struct AbResults
    formula::FormulaTerm
    coef::Matrix{Float64}
    expected::Vector{Float64}
    actual::Vector{Float64}
    abr::Vector{Float64}
    xnames
    yname
end


function simple_reg(resp,pred
    )
    
    coef=cholesky!(Symmetric(pred' * pred)) \ (pred' * resp)
    er=bh_return(pred,coef)
    ar=bh_return(resp)
    # er=1
    # ar=1  # 5.019152 seconds (4.98 M allocations: 1.287 GiB, 3.75% gc time)
    return coef,er,ar
end;


function bh_return(pred, coef)
    
    out = 1.0
    @simd for i in 1:size(pred, 1)
        out *= (fast_pred(pred, coef, i) + 1)
    end
    out - 1
end

function bh_return(vals)
    out = 1.0
    @simd for x in vals
        out *= (1 + x)
    end
    out - 1
end

function fast_pred(pred, coef, i)
    out = 0.0
    @simd for j in 1:length(coef)
        @inbounds out += pred[i, j] * coef[j]
    end
    out
end



""" 
for each line of ds_events,  find the corresponding range in ds_firm,
and do the regression
"""
function binary_search(
    event_firmid_col,
    start_col,
    end_col,
    firm_col,
    date_col,
    obsid_col,
    resp_col,
    p_coef,
    er,
    ar,
    pred;threads=true)
    ## multi-threads for loop, for each firm in dataset
    IMD.@_threadsfor threads  for n in 1:length(event_firmid_col)

      
        i=searchsorted(firm_col,event_firmid_col[n]) 

        s=searchsortedfirst(view(date_col,i), start_col[n])
        # s=searchsortedfirst(date_col, IMD._columns(ds_e)[5][n],i[1],i[end],Base.Order.ForwardOrdering())-i[1]+1
        e=searchsortedlast(view(date_col,i), end_col[n])

        obs_id=view(obsid_col,i[1]+s-1:i[1]+e-1)
        resp=view(resp_col,i[1]+s-1:i[1]+e-1)
        predictor=view(pred,obs_id,:) ## totally 1.85 seconds
        p_coef[n,:],er[n],ar[n]=simple_reg(resp,predictor)## totally 5.4 seconds
    end

end



"""
    function group_and_reg(
        ds_e::InMemoryDatasets.Dataset,
        data::MarketData,
        f::FormulaTerm
    )

Calculates a linear regression for the supplied data based on the formula (formula from StatsModels.jl), 
therefore, the user needs to know the factor information in the market data.

`group_and_reg` is an intentionally simplistic linear regression.For each firm event, it looks up the 
corresponding factors in the firm and market data, and does a regression to calculate the coefficients 
of the formular. It also attempts to produce a minimum number of allocations if views of vectors are 
passed.

# Arguments
- `ds_e`:An InMemoryDatasets.Dataset that stores events data
- `data`: A MarketData structure which stores sorted firm data and market data
- `f`: A StatsModels.jl formula, provided by user

# Example
```@example general
using DLMReader
using MDEStudy
ds_firm = filereader(joinpath(pathof(MDEStudy),"..","..","benchmark","data", "firm_ret.csv"), types = Dict(2=>Date)); 
ds_mkt = filereader(joinpath(pathof(MDEStudy),"..","..","benchmark","data", "mkt_ret.csv"), types = Dict(1=>Date));
ds_events=filereader(joinpath(pathof(MDEStudy),"..","..","benchmark","data","event_dates.csv"),types = Dict(2:6 .=>Date)) |> unique;
mkt_data = MarketData(
ds_mkt,
ds_firm
)
reg_result=group_and_reg(ds_events,data, @formula(ret ~ mkt + smb + hml + umd))
```
"""
function group_and_reg(ds_e::InMemoryDatasets.Dataset,data::MarketData,f::FormulaTerm)

    if !StatsModels.omitsintercept(f) & !StatsModels.hasintercept(f)
        f = FormulaTerm(f.lhs, InterceptTerm{true}() + f.rhs)
    end
    res=Symbol(f.lhs)
    res_idx=IMD.index(data.firmdata)[res]
    m_data=merge(data.marketdata,(;res => IMD._columns(data.firmdata)[res_idx]))

    sch = apply_schema(f, schema(f, m_data,Dict(keys(m_data) .=> ContinuousTerm))) ## take extra 0.9 seconds
    # sch = apply_schema(f, schema(f, m_data))
    resp, pred = modelcols(sch, m_data)
    yname, xnames = coefnames(sch)
    #coefficients
    p_coef=zeros(Float64, nrow(ds_e), size(pred,2))
    #expected return
    er=zeros(Float64, nrow(ds_e))
    #actual return
    ar=zeros(Float64, nrow(ds_e)) ## these pre-allocations take extra 0.2 seconds

    id_idx=IMD.index(data.firmdata)[:firm_id]
    date_idx=IMD.index(data.firmdata)[:date]
    obs_id_idx=IMD.index(data.firmdata)[:obs_id_right]
    event_firmid_idx=IMD.index(ds_e)[:firm_id]
    start_idx=IMD.index(ds_e)[:est_window_start]
    end_idx=IMD.index(ds_e)[:est_window_end]

    firm_col=IMD._columns(data.firmdata)[id_idx]
    date_col=IMD._columns(data.firmdata)[date_idx]
    obsid_col=IMD._columns(data.firmdata)[obs_id_idx]
    resp_col=IMD._columns(data.firmdata)[res_idx]
    event_firmid_col=IMD._columns(ds_e)[event_firmid_idx]
    start_col=IMD._columns(ds_e)[start_idx]
    end_col=IMD._columns(ds_e)[end_idx]

    binary_search(event_firmid_col,start_col,end_col,firm_col,date_col,obsid_col,resp_col,p_coef,er,ar,pred)
    r=AbResults(
        f,
        p_coef,
        er,
        ar,
        ar-er,
        xnames,
        yname
    )
    return r
end;



function Base.show(io::IO, r::AbResults)
    println(io, " the formular is:  $(r.formula)")
    cnames=["Abr", "Actual", "Expected" ]
    cnames=[r.xnames;cnames]
    values=hcat(r.coef,r.abr,r.actual,r.expected)
    d=Dataset(values,:auto)
    rename!(d,cnames)
    print(io,d)
end

