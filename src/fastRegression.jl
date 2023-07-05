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
    evar
    the_date
end


function simple_reg(resp,pred,pred3,resp3
    )
    
    coef=cholesky!(Symmetric(pred' * pred)) \ (pred' * resp)
    er=bh_return(pred,coef)
    ar=bh_return(resp)
    # er=1
    # ar=1  # 5.019152 seconds (4.98 M allocations: 1.287 GiB, 3.75% gc time)

    t=fast_pred_event(pred3,coef)
    event_er=resp3-t
    # print("######:",pred3,"\n")
    # print(resp3,"\t",pred3,"\t",coef,"\t",t,"\t",event_er,"###","\n")
    # print(length(coef),length(pred3))
    # print(event_er)

    return coef,er,ar,event_er
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

function fast_pred_event(pred1, coef1)
    out1 = 0.0
    @simd for z in 1:length(coef1)
        @inbounds out1 += pred1[z] * coef1[z]
    end
    out1
end

function cumulative_return(pred::AbstractMatrix, coef)
    #@assert size(pred, 2) == length(coef) "Got Matrix of size $(size(pred)) and coefficients of $coef $pred"
    out = 0.0
    @simd for i in 1:size(pred, 1)
        out += fast_pred(pred, coef, i)
    end
    out
end
car(resp, pred, coef) = sum(resp) - cumulative_return(pred, coef)


""" 
for each line of ds_events,  find the corresponding range in ds_firm,
and do the regression
"""
function binary_search(
    event_firmid_col,
    event,
    event_start_col,
    event_end_col,
    start_col,
    end_col,
    firm_col,
    date_col,
    obsid_col,
    resp_col,
    p_coef,
    er,
    ar,
    pred,
    evar;threads=true)
    ## multi-threads for loop, for each firm in dataset
    IMD.@_threadsfor threads  for n in 1:length(event_firmid_col)

      
        i=searchsorted(firm_col,event_firmid_col[n]) 

        s=searchsortedfirst(view(date_col,i), start_col[n])
        # s=searchsortedfirst(date_col, IMD._columns(ds_e)[5][n],i[1],i[end],Base.Order.ForwardOrdering())-i[1]+1
        e=searchsortedlast(view(date_col,i), end_col[n])

        obs_id=view(obsid_col,i[1]+s-1:i[1]+e-1)
        resp=view(resp_col,i[1]+s-1:i[1]+e-1)
        predictor=view(pred,obs_id,:) ## totally 1.85 seconds

        ## observations of event_window
        s2=searchsortedfirst(view(date_col,i), event_start_col[n])
        e2=searchsortedlast(view(date_col,i), event_end_col[n])
        obs_id2=view(obsid_col,i[1]+s2-1:i[1]+e2-1)
        resp2=view(resp_col,i[1]+s2-1:i[1]+e2-1)
        predictor2=view(pred,obs_id2,:)
  
        ## observation of event_date
        event_idx=searchsortedfirst(view(date_col,i),event[n])
        # print(searchsorted(view(date_col,i),event[n]),"\t",event[n],"\n")
        resp3=view(resp_col,event_idx)

        predictor3=view(pred,event_idx,:)
        # print(predictor3)
        # print("\n")

        p_coef[n,:],er[n],ar[n],evar[n]=simple_reg(resp,predictor,predictor3,resp3[1])## totally 5.4 seconds

        # car=car(resp, pred, coef)
        # c=cumulative_return(predictor2, p_coef[n,:])

        cuar=car(resp2,predictor2,p_coef[n,:])
        # print(cuar,"\n")
        # print(resp2,"\t",predictor2,"\t",p_coef[n,:],"\t",cuar,"\n")
        # print(c,"\t",length(predictor2),"\t",length(resp2),"\n")
        # print(s2,"\t",e2,"\t",size(predictor2),"\t",obs_id2,"\n")
        # print(cuar,"\n")


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
    #buy and hold expected return
    er=zeros(Float64, nrow(ds_e))
    #buy and hold actual return
    ar=zeros(Float64, nrow(ds_e)) ## these pre-allocations take extra 0.2 seconds



    id_idx=IMD.index(data.firmdata)[:firm_id]
    date_idx=IMD.index(data.firmdata)[:date]
    obs_id_idx=IMD.index(data.firmdata)[:obs_id_right]
    event_firmid_idx=IMD.index(ds_e)[:firm_id]
    start_idx=IMD.index(ds_e)[:est_window_start]
    end_idx=IMD.index(ds_e)[:est_window_end]
    event_start_idx=IMD.index(ds_e)[:event_window_start]
    event_end_idx=IMD.index(ds_e)[:event_window_end]

    event_idx=IMD.index(ds_e)[:date]

    firm_col=IMD._columns(data.firmdata)[id_idx]
    date_col=IMD._columns(data.firmdata)[date_idx]
    obsid_col=IMD._columns(data.firmdata)[obs_id_idx]
    resp_col=IMD._columns(data.firmdata)[res_idx]
    event_firmid_col=IMD._columns(ds_e)[event_firmid_idx]
    start_col=IMD._columns(ds_e)[start_idx]
    end_col=IMD._columns(ds_e)[end_idx]
    event_start_col=IMD._columns(ds_e)[event_start_idx]
    event_end_col=IMD._columns(ds_e)[event_end_idx]

    event=IMD._columns(ds_e)[event_idx]

    # event abnomral return
    # ls=IMD._columns(ds_e)[event_start_idx][1]
    # le=IMD._columns(ds_e)[event_end_idx][1]
    # evar=zeros(Float64,nrow(ds_e),Dates.value(le-ls))
    evar=zeros(Float64, nrow(ds_e))

    # print(BusinessDays.isbday(data.calendar,"2023-03-23"),"\n")
    # print(BusinessDays.bdayscount(data.calendar,Date("2003-02-22"),Date("2003-03-14")))
    

    binary_search(event_firmid_col,event,event_start_col,event_end_col,start_col,end_col,firm_col,date_col,obsid_col,resp_col,p_coef,er,ar,pred,evar)
    r=AbResults(
        f,
        p_coef,
        er,
        ar,
        ar-er,
        xnames,
        yname,
        evar,
        event
    )
    return r
end;



function Base.show(io::IO, r::AbResults)
    println(io, " the formular is:  $(r.formula)")
    cnames=["Date","Abr","bhAbr", "Actual", "bhExpected" ]
    cnames=[r.xnames;cnames]
    values=hcat(r.coef,r.the_date,r.evar,r.abr,r.actual,r.expected)
    d=Dataset(values,:auto)
    rename!(d,cnames)
    print(io,d)
end

