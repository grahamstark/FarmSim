export SUBSIDIES, WORKERS

const SUBSIDIES = [
    :other_environment_grants_and_subsidies, 
    :subsidies, 
    :non_crop_livestock_grants_subsidies, 
    :fadn_current_subsidies_taxes, 
    :general_farm_subsidies_environment_payments, 
    :livestock_sales_subsidies, 
    :other_subs_cam, 
    :crop_sales_subsidies, 
    :agrienv_hfa_subs_cam, 
    :input_subsidies,
    :output_subsidies, 
    :subsidies_payments_to_agriculture, 
    :livestock_subsidies, 
    :livestock_subsidies_check, 
    :dairy_cattle_subsidies, 
    :other_livestock_subsidies, 
    :other_livestock_subsidies_check ]

const WORKERS = [
    :labour_force,
    :manager,
    :working_spouse, 
    :trainees,
    :paid_whole_time_workers, 
    :unpaid_workers, 
    :paid_workers, 
    :time_worked_farmers_partners, 
    :time_worked_farmer, 
    :time_worked_spouse, 
    :time_worked_partners, 
    :time_worked_full_time_workers, 
    :contract_work, 
    :hirework_cam, 
    :sectioni_non_agricultural_hirework_costs, 
    :sectioni_non_agricultural_hirework_output, 
    :paid_part_time_workers, 
    :time_worked_part_time_workers, 
    :agricultural_hirework_output, 
    :agricultural_hirework_costs, 
    :other_unpaid_workers,
    :paid_casual_awu, ]


function redistribute( ad::DataFrame; weight::Symbol, subsidy::Symbol, workers::Symbol, prop::Number )
@argcheck (0 <= prop <= 1) "That's not a prop"
    val = ad[!,weight] .* ad[!,subsidy]
    people = ad[!,weight] .* ad[!,workers]
    val, people
    ad.ub = val ./ people
    sum(val), sum(people)
end

function load(year::Int)::DataFrame
    ad = CSV.File("/mnt/data/fadn/calcdata-20$(year).tab")|>DataFrame
    ad = coalesce.(ad,0)
    ad
end

const var_pattern = r"[a-zA-Z_\.:\[\]0-9\(\)]+"
const op_pattern = r"[\+\-\*\/]"

function parse_one_line( io, r )
    varname = editnames(r.varname)
    # Pattern to capture variables and operators separately

    # Extract all variables
    variables = [m.match for m in eachmatch(var_pattern, r.formula)]
    println( r.formula )
    println(variables)  # ["x", "y", "z", "total", "count"]

    # Extract all operators
    operators = [m.match for m in eachmatch(op_pattern,r.formula)]
    println(operators)  # ["+", "*", "-", "/"]
    nv = length(variables)
    no = length(operators)
    println( io, "#  $(varname) = $(r.formula)")
    print( io, "$varname = ")
    for i in 1:nv
        var = editnames(variables[i])
        print( io, "adm.$(var) " )
        # if i in 2:nv-1
            if i <= no
                print( io, operators[i])
            end
        # end
    end
    println(io,"\n")
end

function parse_calcs()
    io = open( "operators.txt","w")
    calcs = CSV.File( joinpath(DIR,"23calcvars_protect.csv")) |> DataFrame
    rename!( calcs, [:id, :junk, :varname, :formula ])
    for r in eachrow(calcs)
        parse_one_line( io, r )
    end
    close(io)
end