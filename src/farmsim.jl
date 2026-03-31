export SUBSIDIES, WORKERS

const INCOME_MEASURES = [
    :net_income,
    :subsidies
]

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

function last_raw( f :: Farm )::DataFrameRow
    r = f.raw
    lasty = maximum( r.account_year )
    return r[r.account_year .== lasty,:][1,:]
end


@with_kw mutable struct Result
    subsidies = 0.0
    net_income = 0.0    
end


@with_kw mutable struct Params
    bi = 0.0
    bi_include_unpaid = false
    bi_include_paid = false
    tmp_prop_paid = 1.0    
end

function weeklyise!( sys::Params)
    sys.bi /= 52
    sys.tmp_prop /= 100
end

@with_kw mutable struct Settings
    year = 2023
end


function calc_one( farm :: Farm, sys :: Params, settings :: Settings )::Result
    res = Result()
    bi = 0.0
    if sys.bi_include_unpaid
        bi += sys.bi*farm.unpaid_workers
    end
    if sys.bi_include_paid
        bi += sys.bi*farm.paid_workers
    end
    r = last_raw(farm)
    chsubsidy = (1-sys.tmp_prop_paid)*r.net_subsidies_fixed
    res.subsidies = r.net_subsidies_fixed - chsubsidy + bi
    # q: fadn_output includes subsidies?
    res.net_income = r.fadn_output + bi - chsubsidy
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

const GL_COLNAMES = [
    "Lose > 50%",
    "Lose > 25%",
    "Lose > 10%",
    "Lose > 5%",
    "Unchanged",
    "Gain < 10%",
    "Gain < 25%",
    "Gain < 50%",
    "Gain >= 50%"
]

const TABLE_COLNAMES=vcat( "",GL_COLNAMES)

function gl(before::Number, after::Number)::String

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
    i = if pctc < -50
            1
        elseif pctc < -25
            2
        elseif pctc < -10
            3
        elseif pctc < -5
            4
        elseif pctc < 5
            5
        elseif pctc < 10
            6
        elseif pctc < 25
            7
         elseif pctc < 50
            8
        else
            9
        end
    return GL_COLNAMES[i]
end

const MAX_EXAMPLES = 50

function gain_lose_table( 
    dhh :: AbstractDataFrame,
	breakdown::Symbol )::NamedTuple
    colnames = Symbol.([breakdown, GL_COLNAMES...])    
    @show names(dhh)
    max_examples = 2000
    examples = DataFrame(
        farm_number = zeros( Int, max_examples ),
        colval = fill( "", max_examples ),
        rowval = fill( "", max_examples )
    )
    # fill out examples as a dataframe
    nexamples = 0
    for colval in GL_COLNAMES # levels( dhh.gainlose )
        for rowval in sort(levels( dhh[!, breakdown ]))
            subset = dhh[ (dhh[!,breakdown] .== rowval) .& (dhh[!,:gainlose] .== colval),:]
            # this next bit collects at most NUM_EXAMPLES of hhlds with (e.g.) colval='No Change'
            # and row val = "Decile 1", and so on, and appends their details to the `examples` 
            # dataframe.
            ncaserows,ncasecols = size(subset)
            if ncaserows > 0
                nsamples = min(ncaserows,MAX_EXAMPLES)
                # sample nsamples examples 
                subset = subset[sample(1:ncaserows,nsamples,replace=false),:]
                for r in eachrow(subset)
                    nexamples += 1
                    ex = examples[nexamples,:]
                    ex.farm_number = r.farm_number
                    ex.colval = colval
                    ex.rowval = string(rowval)
                end
            end
        end
    end
    examples = examples[1:nexamples,:]
    sort!( examples,[:colval,:rowval])

	ghh = combine( groupby( dhh, [breakdown,:gainlose] ),:weight=>sum)
	sort!( ghh, breakdown)
    vhh = unstack( ghh, :gainlose, :weight_sum )
    @show vhh
    n = size( vhh )[1]
    missn = setdiff( colnames, Symbol.(names(vhh)))
    for m in missn
        vhh[:,m] = zeros(n)
    end
    select!( sort!(vhh, breakdown), colnames... )

    nr,nc = size(vhh)
    #=
    for r in eachrow(vhh)
        for c in 2:nc
            if ismissing(r[c])
                r[c] = 0.0
            end
        end
    end
    =#

    gavch = combine( groupby( dhh, [breakdown]),
        ([:weight]=>sum=>:total_farms),          # sum of hh weights
        ([:pre_income,:weight]=>mean=>:pre_income ),
        ([:post_income,:weight]=>mean=>:post_income ),
        ([:change_subsidies,:weight]=>mean=>:average_change_subsidies ))     # sum of bhc changes
    gavch.avch = gavch.people_weighted_change_sum ./ gavch.weighted_people_sum # => average change for each group per person
    gavch.total_transfer = WEEKS_PER_YEAR.*gavch.weighted_bhc_change_sum./1_000_000 # total moved to/from that group
    # TEMP overwrite the new pct_change for JP -
    gavch.pct_change = 100.0 .* ((gavch.people_weighted_post_income_sum .- gavch.people_weighted_pre_income_sum)./gavch.people_weighted_pre_income_sum)

    # £spa
    vhh = coalesce.(vhh,0.0)

    vhh.average_change_subsidies = gavcg.average_change_subsidies
    vhh.total_change_subsidies = gavcg.total_change_subsidies
    vhh.pct_change_income = gavcg.pct_change_income
    vhh.total_farms = gavcg.total_farms

    return (; table=vhh,examples)
end

function make_gain_lose_tables( 
    before::AbstractDataFrame, 
    after ::AbstractDataFrame,
    change :: Symbol )::NamedTuple
    dhh = deepcopy(before)
    @show names(before)
	dhh.gainlose = gl.(before[!,change], after[!,change])
    gl_farm_type = gain_lose_table( dhh, :farm_type )
    gl_tenure_type = gain_lose_table( dhh, :tenure_type )
    gl_paid_workers = gain_lose_table( dhh, :paid_workers )
    gl_farm_size = gain_lose_table( dhh, :farm_size )
    gl_gor = gain_lose_table( dhh, :gor )
    gl_form_of_business = gain_lose_table( dhh, :form_of_business )
    gl_revenue_quintile = gain_lose_table( dhh, :revenue_quintile )
    gl_outputs_over_inputs_quartile = gain_lose_table( dhh, :outputs_over_inputs_quartile )
    return( ; 
        gl_farm_type, 
        gl_tenure_type, 
        gl_paid_workers, 
        gl_farm_size,
        gl_gor,
        gl_form_of_business,
        gl_revenue_quintile,
        gl_outputs_over_inputs_quartile )
end

function merge( d1::DataFrame, d2::DataFrame )::DataFrame
    adm = hcat(d1,d2;makeunique=true)
end



function summarise_output( output::Vector{DataFrame}, settings :: Settings )::NamedTuple

    gl_subsidies = make_gain_lose_tables( output[1], output[2], :subsidies )
    gl_net_income = make_gain_lose_tables( output[1], output[2], :net_income )
    return (; gl_subsidies, gl_net_income )
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

function zerocost( systems::Vector{Params}, settings :: Settings):Float
    summary,output = calc( systems, settings :: Settings; reset=false )
    s1 = sum( output[1].subsidies, Weights( output[1].weight ))
    s2 = sum( output[2].subsidies, Weights( output[2].weight ))
    return 1-((s2-s1)/s1)
end

function redistribute( ad::DataFrame; weight::Symbol, subsidy::Symbol, workers::Symbol, prop::Number )
@argcheck (0 <= prop <= 1) "That's not a prop"
    val = ad[!,weight] .* ad[!,subsidy]
    people = ad[!,weight] .* ad[!,workers]
    val, people
    ad.ub = val ./ people
    sum(val), sum(people)
end


# for prettytables
function fm(v, r,c) 
    return if c == 1
        v
    elseif c < 12
        Format.format(v, precision=0, commas=true)
    else
        Format.format(v, precision=2, commas=true)
    end
    s
end

function pretty(s)
    s
end

function format_gl( io, title::String, sf :: DataFrame; backend=:markdown )
    sf[!,1] = pretty.(sf[!,1]) # labels on RHS
    # io = IOBuffer()
    pretty_table( 
        io, 
        sf[!,1:end]; 
        backend = backend,
        formatters=[fm], 
        col_labels=TABLE_COLNAMES,
        alignment=[:l,fill(:r,9)...],
        # highlighters = [ht],
        title = title )
    # return String(take!(io))
end

function tables_to_md( out, title, tabs )
    println( out, "# $title")
    format_gl( out, "Farm Type", tabs.gl_farm_type.table ) 
    format_gl( out, "Tenure Type", tabs.gl_tenure_type.table ) 
    format_gl( out, "# Paid Workers", tabs.gl_paid_workers.table ) 
    format_gl( out, "Farm Size", tabs.gl_farm_size.table )
    format_gl( out, "Region", tabs.gl_gor.table )
    format_gl( out, "Form of Business", tabs.gl_form_of_business.table )
    format_gl( out, "Revenue Quintile (5=highest)", tabs.gl_revenue_quintile.table )
    format_gl( out, "Outputs Over Inputs Quartile (higher=more efficient)", tabs.gl_outputs_over_inputs_quartile.table )  
end



function tables_to_typst( out, title, tabs )
    println( out, "= $title")
    format_gl( out, "Farm Type", tabs.gl_farm_type.table; backend=:typst )
    format_gl( out, "Tenure Type", tabs.gl_tenure_type.table; backend=:typst )
    format_gl( out, "# Paid Workers", tabs.gl_paid_workers.table; backend=:typst )
    format_gl( out, "Farm Size", tabs.gl_farm_size.table; backend=:typst )
    format_gl( out, "Region", tabs.gl_gor.table; backend=:typst )
    format_gl( out, "Form of Business", tabs.gl_form_of_business.table; backend=:typst )
    format_gl( out, "Revenue Quintile (5=highest)", tabs.gl_revenue_quintile.table; backend=:typst )
    format_gl( out, "Outputs Over Inputs Quartile (higher=more efficient)", tabs.gl_outputs_over_inputs_quartile.table; backend=:typst )
end

