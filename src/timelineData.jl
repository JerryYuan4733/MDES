

struct MarketData
    calendar::MarketCalendar
    firmdata
    marketdata
end


function MarketData(
    ds_market,
    ds_firms;
    date_col_market=:date,
    date_col_firms=:date,
    id_col=:firm_id,
    valuecols_market=nothing,
    valuecols_firms=nothing
    )
    ## if the target columns are not specified
    if valuecols_market === nothing
        valuecols_market = Symbol.([n for n in Symbol.(names(ds_market)) if n ∉ [date_col_market]])
    end
    if valuecols_firms === nothing
        valuecols_firms = Symbol.([n for n in Symbol.(names(ds_firms)) if n ∉ [date_col_firms, id_col]])
    end

    if id_col!=:firm_id
        IMD.rename!(ds_firms,Dict(id_col => :firm_id ))
    end


    #@ select the columns we interested
    IMD.select!(ds_market, vcat([date_col_market], valuecols_market))
    # IMD.disallowmissing!(ds_market)
    ## sorted by date column 
    IMD.sort!(ds_market,date_col_market)

    ## make sure the date column name is ":date"
    if date_col_market!=:date
        IMD.rename!(ds_market,Dict(date_col_market => :date ))
    end
    ## change the data into NamedTuple type
    market_data=NamedTuple(valuecols_market .=> Tables.columns(ds_market[!,valuecols_market]))

    ## check if there are duplicate rows
    if !allunique(ds_market[!,date_col_market])
        @error("There are duplicate date rows in the market data")
    end

    ## select the columns we interested from firms dataset
    IMD.select!(ds_firms, vcat([id_col, date_col_firms], valuecols_firms))
    # IMD.disallowmissing!(ds_firms)
    IMD.sort!(ds_firms, [id_col, date_col_firms])
    ## make sure the date column name is ":date"
    if date_col_firms!=:date
        IMD.rename!(ds_firms,Dict(date_col_firms => :date ))
    end

    # if !allunique(IMD.modify(df_firms, (id_col,date_col_firms) => byrow((x,y)-> (string(x),string(y))))[!,end])
    #     @error("There are duplicate id-date rows in the firm data")
    # end
  
    ## leftjoin based on :date column 
    IMD.leftjoin!(ds_firms, ds_market[!, [:date]], on = :date, obs_id = [false, true])
    
    ## get the index of the :date column from market and firm dataset
    idx=IMD.index(ds_market)[date_col_market]
    idx2=IMD.index(ds_firms)[date_col_firms]

    ## using date info from marke dataset to create a self-design calendar which have the bussiness days info
    cal = MarketCalendar(IMD._columns(ds_market)[idx])

    ## using the calendar above to check if all dates in firms data are business days
    check_all_businessdays(convert(Vector{Date},unique(IMD._columns(ds_firms)[idx2])), cal)

    ## return a structure which has business days info, market and firms data
    MarketData(
        cal,
        ds_firms,
        market_data
        # ds_market
    )
end


function check_all_businessdays(dates, cal)
    bday_list = isbday(cal, dates)
    if !all(bday_list)
        bday_list_inv = (!).(bday_list)
        if sum(bday_list_inv) <= 3
            @error("Dates $(dates[bday_list_inv]) are not in the MARKET_DATA_CACHE")
        else
            d1_test = findfirst(bday_list_inv)
            d2_test = findlast(bday_list_inv)
            @error("Dates $(dates[d1_test]) ... $(dates[d2_test]) are not in the MARKET_DATA_CACHE")
        end
    end
end



function Base.show(io::IO, data::MarketData)
    println(io, "Calendar: ")
    println(io, data.calendar)
    println(io, "Head of Firm Data: ")
    println(io, first(data.firmdata,4))
    println(io, "Market Data: ")
    println(io, data.marketdata)
    
end