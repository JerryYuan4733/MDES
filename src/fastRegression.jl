function simple_reg(obs_id,resp,predictors,
    )

    pred = view(predictors,obs_id,:)
    # fit=lm(f,m)
    coef=cholesky!(Symmetric(pred' * pred)) \ (pred' * resp)
    er=bh_return(pred,coef)
    ar=bh_return(resp)
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

function group_and_reg(ds_e,data::MarketData,f::FormulaTerm; threads = true )

    res=Symbol(f.lhs)
    res_idx=IMD.index(data.firmdata)[res]
    m_data=merge(data.marketdata,(;res => IMD._columns(data.firmdata)[res_idx]))

    # sch = apply_schema(f, schema(f, m_data,Dict(:ret => ContinuousTerm,:mkt => ContinuousTerm,:hml => ContinuousTerm,:smb => ContinuousTerm,:umd => ContinuousTerm)))
    sch = apply_schema(f, schema(f, m_data,Dict(keys(m_data) .=> ContinuousTerm)))
    resp, pred = modelcols(sch, m_data)
    # yname, xnames = coefnames(sch)

    #coefficients
    p_coef=zeros(Float64, nrow(ds_e), size(pred,2))
    #expected return
    er=zeros(Float64, nrow(ds_e))
    #actual return
    ar=zeros(Float64, nrow(ds_e))

    IMD.@_threadsfor threads  for n in 1:nrow(ds_e)

        id_idx=IMD.index(data.firmdata)[:firm_id]
        i=searchsorted(IMD._columns(data.firmdata)[id_idx],IMD._columns(ds_e)[1][n]) 

        date_idx=IMD.index(data.firmdata)[:date]
        e=searchsortedlast(view(IMD._columns(data.firmdata)[date_idx],i), IMD._columns(ds_e)[6][n])
        s=searchsortedfirst(view(IMD._columns(data.firmdata)[date_idx],i), IMD._columns(ds_e)[5][n])


        obs_id_idx=IMD.index(data.firmdata)[:obs_id_right]
        obs_id=view(IMD._columns(data.firmdata)[obs_id_idx],i[1]+s-1:i[1]+e-1)

        resp=view(IMD._columns(data.firmdata)[res_idx],i[1]+s-1:i[1]+e-1)

        p_coef[n,:],er[n],ar[n]=simple_reg(obs_id,resp,pred)
 
        
    end
    
    return p_coef,er,ar
end;


