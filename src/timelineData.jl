abstract type CalendarData end


struct DataVector <: CalendarData
    data::Vector{Float64}
    missing_bdays::SparseVector{Bool, Int}
    dates::ClosedInterval{Date}
    calendar::MarketCalendar
    function DataVector(data, missing_bdays, dates, calendar)
        @assert length(data) == length(missing_bdays) == bdayscount(calendar, dates.left, dates.right) + 1 "Data does not match length of dates or missings"
        new(data, missing_bdays, dates, calendar)
    end
end


struct DataMatrix <: CalendarData
    data::Matrix{Float64}
    missing_bdays::SparseVector{Bool, Int}# corresponds to each row with a missing value
    dates::ClosedInterval{Date}
    calendar::MarketCalendar
    function DataMatrix(data, missing_bdays, dates, calendar)
        @assert size(data, 1) == length(missing_bdays) == bdayscount(calendar, dates.left, dates.right) + 1 "Data does not match length of dates or missings"
        new(data, missing_bdays, dates, calendar)
    end
end

struct MarketData{T, MNames, FNames, N1, N2}
    calendar::MarketCalendar
    marketdata::NamedTuple{MNames, NTuple{N1, DataVector}} # column names as symbols
    firmdata::Dict{T, NamedTuple{FNames, NTuple{N2, DataVector}}} # data stored by firm id and then by column name as symbol
end
