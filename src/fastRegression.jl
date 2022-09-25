function simple_reg(obs_id,y,m
    )

    pred = view(m,obs_id,:)
    resp = y
    coef=cholesky!(Symmetric(pred' * pred)) \ (pred' * resp)
    er=bh_return(pred,coef)
    ar=bh_return(resp)
    return coef,er,ar
end;



function bh_return(pred::AbstractMatrix, coef)
    
    out = 1.0
    @simd for i in 1:size(pred, 1)
        out *= (fast_pred(pred, coef, i) + 1)
    end
    out - 1
end

function bh_return(vals::AbstractVector{Float64})
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
    # if !StatsModels.omitsintercept(f) & !StatsModels.hasintercept(f)
    #     f = FormulaTerm(f.lhs, InterceptTerm{true}() + f.rhs)
    # end

    m_data=merge(data.marketdata,(;:ret => IMD._columns(data.firmdata)[3]))

    sch = apply_schema(f, schema(f, m_data))
    # select!(data, internal_termvars(sch))
    resp, pred = modelcols(sch, m_data)
    # yname, xnames = coefnames(sch)

    p_coef=zeros(Float64, nrow(ds_e), 4)
    er=zeros(Float64, nrow(ds_e))
    ar=zeros(Float64, nrow(ds_e))

    IMD.@_threadsfor threads  for n in 1:nrow(ds_e)

        # i=searchsorted(IMD._columns(ds_f)[1],view(ds_e,n,:firm_id)[1])#39M
        i=searchsorted(IMD._columns(data.firmdata)[1],IMD._columns(ds_e)[1][n]) 


        e=searchsortedlast(view(IMD._columns(data.firmdata)[2],i), IMD._columns(ds_e)[6][n])
        s=searchsortedfirst(view(IMD._columns(data.firmdata)[2],i), IMD._columns(ds_e)[5][n])



        obs_id=view(IMD._columns(data.firmdata)[4],i[1]+s-1:i[1]+e-1)

        # obs_id=view(obs_ids,i[1]+s-1:i[1]+e-1)
        # ret=IMD._columns(view(ds_f,i[1]+s-1:i[1]+e-1,3:3))[1]
        ret=view(IMD._columns(data.firmdata)[3],i[1]+s-1:i[1]+e-1)
        
        p_coef[n,:],er[n],ar[n]=simple_reg(obs_id,ret,pred)
 
        
    end
    
    return p_coef,er,ar
end;


