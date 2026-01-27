module FarmSim
#= 

Start of Simple England-only Farm income model, eventually looking like 

=#
using Reexport
using Parameters: @with_kw

@reexport using ArgCheck
@reexport using CategoricalArrays
@reexport using Chain
@reexport using CSV
@reexport using DataFrames
@reexport using DataStructures
@reexport using Dates
@reexport using Format
@reexport using PanelDataTools
@reexport using StatsBase
@reexport using PrettyTables

# we've promised to use this only one OneDrive, so....
const DDIR_ONEDRIVE = joinpath( "C:\\","Users","gwdv3","OneDrive - Northumbria University - Production Azure AD","Documents","Basic_Income_Farmers","FarmBusinessSurvey","data" )
const SYNTH_DATA = joinpath( "/", "mnt", "data", "farm-microsimulation")
# each year unpacked into own directory
const DATADIRS = OrderedDict([
    2021=>joinpath(DDIR_ONEDRIVE, "9041txt", "UKDA-9041-txt", "txt"),
    2022=>joinpath(DDIR_ONEDRIVE, "9287txt", "UKDA-9287-txt", "txt","standard_output_coefficients_2017_version"), # why? who knows ..
    2023=>joinpath(DDIR_ONEDRIVE, "9360txt", "UKDA-9360-txt", "txt")])


const DIR="/mnt/data/fadn/"

const COMBINED_CALCDATA = joinpath( DDIR_ONEDRIVE,"edited", "calcdata-2012-2023-combined.tab" )

export 
    to_i,
    load_calcdata_as_panel,
    open_raw_files, 
    wrangle_datasets,
    namesearch,
    by_year_averages,
    load_from_joined, 
    by_year_averages, 
    add_productivty_quintiles!

include( "fbs.jl")
include( "farmsim.jl")

#=
farm_type
altitude
tenure_type
gor
paid_workers
unpaid_workers
rural_classification
farm_size 
epub_farmer_education
adm.farmer_household_total_income
form_of_business
"farmer_age_band"
"farmer_awu"
"farmer_education"
"farmer_gender"
"farmer_has_spouse"
"farmer_house_adults_with_income_not_farmer_spouse"
"farmer_household_drawings"
"farmer_household_drawings_pc"
"farmer_household_farm_income"
"farmer_household_farmer_spouse_income"
"farmer_household_has_spouse"
"farmer_household_income_other_members"
"farmer_household_mcclements_scale"
"farmer_household_mcclements_scale_part1"
"farmer_household_mcclements_scale_part2"
"farmer_household_number"
"farmer_household_number_of_adults"
"farmer_household_number_of_children"
"farmer_household_number_of_occupants"
"farmer_household_number_of_pension_age"
"farmer_household_number_of_working_age"
"farmer_household_oecd_scale"
"farmer_household_total_income"
"farmer_spouse_awu"
"farmer_spouse_labour_cost"
"farmer_spouse_off_farm_earned_income"
=#

end
