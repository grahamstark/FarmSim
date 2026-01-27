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


export Result, Params, Settings, Farm, calc_one, calc, initialise

@with_kw mutable struct Farm
    farm_number=-1
    farm_type=""
    tenure_type=""
    gor=""
    paid_workers=-1
    unpaid_workers=-1
    rural_classification=""
    farm_size=""
    epub_farmer_education="" # epub_farmer_education
    farmer_household_total_income=-1
    form_of_business=""
    outputs_over_inputs_quartile = -1
    revenue_quintile = -1 
    weight=-1.0
    workers=-1.0 # 1/2 workers and so on
    raw = DataFrame()
end


@with_kw mutable struct Result
    subsidies = 0.0
    net_income = 0.0    
end

@with_kw mutable struct Params
    bi = 0.0
end

@with_kw mutable struct Settings
    year = 2023
end


function calc_one( farm :: Farm, sys :: Params, settings :: Settings )::Result
    res = Result()

    return res
end

FARMS = Farm[]

function load(year::Int)::DataFrame
    ad = CSV.File("/mnt/data/fadn/calcdata-$(year).tab")|>DataFrame
    ad = coalesce.(ad,0)
    ad
end

function make_output( nfarms :: Int, nsys :: Int )::Vector{DataFrame}
    out = Vector{DataFrame}(undef,nsys)
    for i in 1:nsys
        out[i] = DataFrame( 
            farm_number = fill(0,nfarms), 
            farm_type = fill("",nfarms),
            tenure_type=fill("",nfarms),
            gor=fill("",nfarms),
            paid_workers=fill(0,nfarms),
            unpaid_workers=fill(0,nfarms),
            workers=fill(0.0,nfarms),
            rural_classification=fill(0,nfarms),
            farm_size=fill("",nfarms),
            epub_farmer_education=fill("",nfarms),
            farmer_household_total_income=zeros(nfarms),
            form_of_business=fill("",nfarms),
            outputs_over_inputs_quartile = fill(0,nfarms),
            revenue_quintile = fill(0,nfarms),
            weight=zeros(nfarms), 
            subsidies=zeros(nfarms),
            net_income=zeros(nfarms))
    end
    return out
end

function initialise( settings::Settings, nsys :: Int; reset=false )
    global FARMS
    if( length(FARMS) == 0)||reset
        adm, adm_avgs, b1, b2, b3, grouped_farms = load_from_joined()
        # df = load( settings.year )

        n = length(grouped_farms)
        FARMS = Vector{Farm}(undef,n)
        i = 0
        for i in 1:n
            FARMS[i] = Farm()
            r = grouped_farms[i]
            f = FARMS[i]
            #= If we're limiting the averages to > 1 observation then do:
            ravg = if size(ravg)[1] > 0 # any average 
                c_ravg[1,:]
            else
                r[end,:]
            end
            =#
            # the String() here is to override the funny string7.. types DataFrame uses
            f.farm_number = r[1,:farm_number]
            ravg = adm_avgs[f.farm_number.==adm_avgs.farm_number,:][1,:]
            f.farm_type=String(r[1,:farm_type])
            f.tenure_type = String(r[1,:tenure_type])
            f.gor = String(r[1,:gor])
            f.paid_workers = r[1,:paid_workers]
            f.unpaid_workers = r[1,:unpaid_workers]
            f.workers = f.unpaid_workers + f.paid_workers
            f.rural_classification = r[1,:rural_classification]
            f.farm_size = String(r[end,:farm_size])
            f.epub_farmer_education = String(r[1,:epub_farmer_education]) # epub_farmer_education
            f.farmer_household_total_income = ravg.farmer_household_total_income
            f.form_of_business = String(r[1,:form_of_business])
            
            f.weight = r[end,:weight]
            f.outputs_over_inputs_quartile = ravg.outputs_over_inputs_quartile
            # @show ravg.revenue_quintile
            f.revenue_quintile = ravg.revenue_quintile

            f.raw = r
        end
    end
    return make_output( length(FARMS), nsys )
end

function add_to_output!( output::DataFrame, farm::Farm, res :: Result, row::Int, settings::Settings )
    r = output[row,:]
    r.farm_number = farm.farm_number    
    r.farm_type = farm.farm_type
    r.weight = farm.weight

    r.tenure_type = farm.tenure_type
    r.gor = farm.gor
    r.paid_workers = farm.paid_workers
    r.unpaid_workers = farm.unpaid_workers
    r.workers = farm.workers
    r.rural_classification = farm.rural_classification
    r.farm_size = farm.farm_size
    r.epub_farmer_education = farm.epub_farmer_education
    r.farmer_household_total_income = farm.farmer_household_total_income
    r.form_of_business = farm.form_of_business
    r.outputs_over_inputs_quartile  = farm.outputs_over_inputs_quartile 
    r.revenue_quintile  = farm.revenue_quintile 
    r.weight = farm.weight

    r.net_income = res.net_income
    r.subsidies = res.subsidies
end

function gl( after::Number, before::Number)::String

    function pct()
         den = if before > 0
           before
         elseif after > 0
           after
         else
          1.0
         end
         return 100*(after-before)/den
    end

    pctc = pct()
    return if pctc < -50
            "Lose > 50%"
        elseif pctc < -25
            "Lose > 25%"
        elseif pctc < -10
            "Lose > 10%"
        elseif pctc < -5
            "Lose > 5%"
        elseif pctc < 5
            "Unchanged"
        elseif pctc < 10
            "Gain < 10%"
        elseif pctc < 25
            "Gain < 25%"
         elseif pctc < 50
           "Gain < 50%"
        else
            "Gain >= 50%"
        end
end

function gain_lose_table( 
	preres::AbstractDataFrame,
    postres::AbstractDataFrame;
    measure::Symbol, 
	breakdown=:farm_type,
	weight=:weight )::AbstractDataFrame

    dhh = DataFrame( 
        hid = preres.hid,
        data_year  = preres.data_year,
        weighted_people = preres.weighted_people,
        weight = preres.weight,
        tenure = preres.tenure, 
        region = preres.region,
        decile = preres.decile,
        hh_type = preres.hh_type,
        num_children = Int.(preres.num_children),            
        in_poverty = preres.in_poverty,
        change = postres[:, measure] - preres[:,measure],
        pre_income = preres[:,measure],
        post_income = postres[:,measure],
    
	ghh = combine(groupby( dhh, breakdown ),([measure,weight]=>wmean=>:measure)))

	sort!( ghh, :measure)
	vhh = unstack( ghh, :account_year, :measure )

    return vhh
end

function merge( d1::DataFrame, d2::DataFrame )::DataFrame
    adm = hcat(d1,d2;makeunique=true)
end


function summarise_output( output::Vector{DataFrame}, settings :: Settings )::NamedTuple


    return (; a=0 )
end

function calc( systems::Vector{Params}, settings :: Settings; reset=false )
    global FARMS
    output = initialise( settings, length(systems); reset=reset )
    row = 0
    for farm in FARMS
        row += 1
        sysno = 0
        for sys in systems
            sysno += 1
            res = calc_one( farm, sys, settings )
            add_to_output!( output[sysno], farm, res, row, settings )
        end
    end
    summary = summarise_output( output, settings )
    return (;summary, output )
end

function redistribute( ad::DataFrame; weight::Symbol, subsidy::Symbol, workers::Symbol, prop::Number )
@argcheck (0 <= prop <= 1) "That's not a prop"
    val = ad[!,weight] .* ad[!,subsidy]
    people = ad[!,weight] .* ad[!,workers]
    val, people
    ad.ub = val ./ people
    sum(val), sum(people)
end

