module AbnormalReturnPkg
using Reexport
using Dates
using InMemoryDatasets
@reexport  using BusinessDays
using IntervalSets: ClosedInterval, (..)
using LinearAlgebra
using Tables
# using DataFrames
# using GLM
@reexport using StatsModels



export MarketCalendar
export MarketData
export group_and_reg

include("marketCalendar.jl")

include("timelineData.jl")

include("fastRegression.jl")

end
