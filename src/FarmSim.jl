module FarmSim
#= 

Start of Simple England-only Farm income model, eventually looking like 

=#
using Reexport

@reexport using ArgCheck
@reexport using CategoricalArrays
@reexport using Chain
@reexport using CSV
@reexport using DataFrames
@reexport using DataStructures
@reexport using Dates
@reexport using PanelDataTools
@reexport using StatsBase

# we've promised to use this only one OneDrive, so....
const DDIR_ONEDRIVE = joinpath( "C:\\","Users","gwdv3","OneDrive - Northumbria University - Production Azure AD","Documents","Basic_Income_Farmers","FarmBusinessSurvey","data" )
const SYNTH_DATA = joinpath( "/", "mnt", "data", "farm-microsimulation")
# each year unpacked into own directory
const DATADIRS = OrderedDict([
    2021=>joinpath(DDIR_ONEDRIVE, "9041txt", "UKDA-9041-txt", "txt"),
    2022=>joinpath(DDIR_ONEDRIVE, "9287txt", "UKDA-9287-txt", "txt","standard_output_coefficients_2017_version"), # why? who knows ..
    2023=>joinpath(DDIR_ONEDRIVE, "9360txt", "UKDA-9360-txt", "txt")])

const COMBINED_CALCDATA = joinpath( DDIR_ONEDRIVE,"edited", "calcdata-2012-2023-combined.tab" )

include( "fbs.jl")
include( "farmsim.jl")

end
