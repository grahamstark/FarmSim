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
    revenue = 0.0
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
    res.subsidies = r.net_subsidies_fixed - chsubsidy + bi # net_subsidies_fixed
    # q: fadn_output includes subsidies?
    res.net_income = r.farm_business_income + bi - chsubsidy # or: r.fadn_output for gross output
    res.revenue = r.fadn_output + bi - chsubsidy
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
            paid_workers_summary = fill("",nfarms),
            unpaid_workers=fill(0,nfarms),
            workers=fill(0.0,nfarms),
            workers_summary = fill("",nfarms),
            rural_classification=fill(0,nfarms),
            farm_size=fill("",nfarms),
            epub_farmer_education=fill("",nfarms),
            farmer_household_total_income=zeros(nfarms),
            form_of_business=fill("",nfarms),
            outputs_over_inputs_quartile = fill(0,nfarms),
            revenue_quintile = fill(0,nfarms),
            weight=zeros(nfarms), 
            subsidies=zeros(nfarms),
            revenue=zeros(nfarms),
            net_income=zeros(nfarms))
    end
    return out
end

function workers_str( i :: Number )::String
    return if i == 0
       "0"
    elseif i == 1
       "1"
    elseif i == 2
        "2"
    elseif i < 5
        "3-4"
    elseif i < 8
        "5-7"
    elseif i < 11
        "8-10"
    else
        ">10"
    end
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

    r.paid_workers_summary = workers_str(farm.paid_workers)
    r.workers_summary = workers_str(farm.workers)

    r.net_income = res.net_income
    r.subsidies = res.subsidies
    r.revenue = res.revenue
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
    "Gain >=50%"
]

const TABLE_COLNAMES=["",GL_COLNAMES...,"Total Farms","Avg. Income","Avg. Change","% Change"]

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

# vmean(x,y) = round(mean(x,Weights(y));digits=1)

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
    n = size( vhh )[1]
    missn = setdiff( colnames, Symbol.(names(vhh)))
    for m in missn
        vhh[:,m] = zeros(n)
    end
    select!( sort!(vhh, breakdown), colnames... )
    nr,nc = size(vhh)
    vhh = coalesce.(vhh,0.0)
    gavch = combine( groupby( dhh, [breakdown]),
        ([:weight]=>sum=>:total_farms),
        ([:pre_changevar,:weight]=>vmean=>:pre_changevar ),
        ([:post_changevar,:weight]=>vmean=>:post_changevar ))
    sort!( gavch, breakdown)
    @show gavch
    vhh.total_farms = gavch.total_farms
    vhh.average_changevar = gavch.pre_changevar
    vhh.average_change_changevar = gavch.post_changevar - gavch.pre_changevar
    vhh.pct_change_changevar = 100.0 .* (gavch.post_changevar - gavch.pre_changevar) ./ gavch.pre_changevar
    return (; table=vhh,examples)
end

"""
Make gain lose and psu a totals row in to the bottom
"""
function make_gain_lose_table( dhh :: DataFrame, breakdown::Symbol, totals::DataFrame ) :: NamedTuple
    t = gain_lose_table( dhh, breakdown )
    @show names(t.table)
    rename!( totals, [1=>names(t.table)[1]]) # so we can merge totals row
    @show names( totals )
    push!( t.table, totals[1,:]; promote=true )
    @show t.table
    return t
end

function make_gain_lose_tables( 
    before::AbstractDataFrame, 
    after ::AbstractDataFrame,
    change :: Symbol )::NamedTuple
    dhh = deepcopy(before)
    nrows, ncols = size(dhh)
    dhh.total = fill( "Total", nrows )
    @show names(before)
	dhh.gainlose = gl.(before[!,change], after[!,change])
	dhh.pre_changevar = before[!,change]
    dhh.post_changevar = after[!,change]

    gl_total = gain_lose_table( dhh, :total ).table
    gl_farm_type = make_gain_lose_table( dhh, :farm_type, gl_total )
    gl_tenure_type = make_gain_lose_table( dhh, :tenure_type, gl_total )
    gl_paid_workers = make_gain_lose_table( dhh, :paid_workers_summary, gl_total )
    gl_workers = make_gain_lose_table( dhh, :workers_summary, gl_total )
    gl_farm_size = make_gain_lose_table( dhh, :farm_size, gl_total )
    gl_gor = make_gain_lose_table( dhh, :gor, gl_total )
    gl_form_of_business = make_gain_lose_table( dhh, :form_of_business, gl_total )
    gl_revenue_quintile = make_gain_lose_table( dhh, :revenue_quintile, gl_total )
    gl_outputs_over_inputs_quartile = make_gain_lose_table( dhh, :outputs_over_inputs_quartile, gl_total )
    return( ; 
        gl_total,
        gl_farm_type, 
        gl_tenure_type, 
        gl_paid_workers, 
        gl_workers,
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
    gl_revenue = make_gain_lose_tables( output[1], output[2], :revenue )
    return (; gl_subsidies, gl_net_income, gl_revenue )
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

function gl_to_row_pcts( df :: DataFrame )
    dfc = deepcopy(df)
    for r in eachrow( dfc)
        for c in 2:10
            r[c] *= 100/r[11]
        end
    end
    dfc
end

"""

"""
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


# , BellCentennial LT Address , flipped: true) , stretch: 75%
const TYPST_PREAMBLE = """

#set page(paper: "a3" )

#set text(
  font: "Palatino Linotype",
  size:10pt
)

#show table: set text(font: "Azo Sans", size:7pt)

#set table(
    columns: (20em, auto, auto),
    align: (left, left, left),
    inset: (x: 8pt, y: 4pt),
    stroke: (x, y) => {if y <= 1 { (top: 0.5pt) }},
    fill: (x, y) => if y > 0 and calc.rem(y, 2) == 0  { rgb("#eee") },
  )
"""


function format_gl( io, title::String, sf :: DataFrame; backend=:markdown, cell_prec=0 )


    h1 = TypstHighlighter( ( data, r, c ) -> (c == 1), ["text-fill"=>"blue"])

    function f_gainlose( h, data, r, c )
        d = Pair{String,String}[]
        colour = if c == 1
            "blue"
        elseif c >= 13
            if data[r,c] < -0.1
                "maroon"
            elseif data[r,c] > 0.1
                "olive"
            else
                "black"
            end
        # elseif (r== size( sf )[1])
          #  "blue"
        else
            "black"
        end
        push!(d, "text-fill" => colour)
        if r == size( sf )[1]
            push!(d, "fill" => "silver")
        end
        if(c == 1) || (r== size( sf )[1])
            push!(d, "text-weight" => "bold")
        end
        # push!(d, "stretch"=>"75%")
        return d
    end

    """
    format cols at end green for good, red for bad.
    """
    hgainlosecols = TypstHighlighter( (data, r, c)->true,  f_gainlose ) # (c >= 13)
    # htotal = TypstHighlighter( (data, r, c)->(r == size( sf )[1]), ["fill"=>"silver", "text-fill"=>"navy"] )



    # for prettytables
    function fm(v, r, c)
        return if c == 1
            v
        elseif v == 0
            "-"
        elseif c < 14
            Format.format(v, precision=cell_prec, commas=true)
        else
            Format.format(v, precision=2, commas=true)
        end
        s
    end


    function pretty(s)
        s
    end

    sf[!,1] = pretty.(sf[!,1]) # labels on RHS
    nrows,ncols = size( sf )
    tb = TypstTableBorders(
        top_line="0pt",
        header_line = "0pt",
        merged_header_cell_line = "0pt",
        middle_line = "0pt",
        bottom_line = "0pt",
        left_line = "0pt",
        center_line = "0pt",
        right_line = "0pt" )
    t = TypstTableFormat(borders=tb, vertical_lines_at_data_columns= :none)
    # io = IOBuffer()

    pretty_table( 
        io, 
        sf[!,1:end]; 
        backend = backend,
        formatters=[fm], 
        highlighters = [hgainlosecols],
        column_labels=TABLE_COLNAMES,
        alignment=[:l,fill(:r,ncols-1)...],
        table_format=t,
        # highlighters = [ht],
        title = title )

end

function tables_to_md( out, title, tabs )
    println( out, "# $title")
    format_gl( out, "Farm Type", tabs.gl_farm_type.table ) 
    format_gl( out, "Tenure Type", tabs.gl_tenure_type.table ) 
    format_gl( out, "Number of Paid Workers", tabs.gl_paid_workers.table )
    format_gl( out, "Farm Size", tabs.gl_farm_size.table )
    format_gl( out, "Region", tabs.gl_gor.table )
    format_gl( out, "Form of Business", tabs.gl_form_of_business.table )
    format_gl( out, "Revenue Quintile (5=highest)", tabs.gl_revenue_quintile.table )
    format_gl( out, "Outputs Over Inputs Quartile (higher=more efficient)", tabs.gl_outputs_over_inputs_quartile.table )  
end



function dump_tables_to_typst_and_csv( outname::String, title::String, tabs )
    out = open( "$(outname).typ", "w")
    csvf = IOBuffer()
    println( out, TYPST_PREAMBLE )
    println( out, "= $title")
    println( csvf, "$title")

    format_gl( out, "Farm Type", tabs.gl_farm_type.table; backend=:typst )
    format_gl( out, "Farm Type - Row %s", gl_to_row_pcts( tabs.gl_farm_type.table ); backend=:typst, cell_prec=1 )
    println( csvf, "\n\n# Farm Type\n\n")
    CSV.write( csvf, tabs.gl_farm_type.table; delim='\t', append=true, writeheader=true, header=TABLE_COLNAMES)

    format_gl( out, "Tenure Type", tabs.gl_tenure_type.table; backend=:typst )
    format_gl( out, "Tenure Type - Row %s", gl_to_row_pcts( tabs.gl_tenure_type.table); backend=:typst, cell_prec=1 )
    println( csvf, "\n\n# Tenure Type\n\n")
    CSV.write( csvf, tabs.gl_tenure_type.table; delim='\t', append=true, writeheader=true, header=TABLE_COLNAMES)

    println( out, "#pagebreak()")
    format_gl( out, "Paid Workers", tabs.gl_paid_workers.table; backend=:typst )
    format_gl( out, "Paid Workers - Row %s",  gl_to_row_pcts( tabs.gl_paid_workers.table ); backend=:typst, cell_prec=1 )
    println( csvf, "\n\n# Paid Workers\n\n")
    CSV.write( csvf, tabs.gl_paid_workers.table; delim='\t', append=true, writeheader=true, header=TABLE_COLNAMES)

    format_gl( out, "All Workers, Inc. Owners", tabs.gl_workers.table; backend=:typst )
    format_gl( out, "All Workers, Inc. Owners - Row %s", gl_to_row_pcts( tabs.gl_workers.table ); backend=:typst, cell_prec=1 )
    println( csvf, "\n\n# All Workers, Inc. Owners\n\n")
    CSV.write( csvf, tabs.gl_workers.table; delim='\t', append=true, writeheader=true, header=TABLE_COLNAMES)

    format_gl( out, "Farm Size", tabs.gl_farm_size.table; backend=:typst )
    format_gl( out, "Farm Size - Row %s", gl_to_row_pcts( tabs.gl_farm_size.table ); backend=:typst, cell_prec=1 )
    println( csvf, "\n\n# Farm Size\n\n")
    CSV.write( csvf, tabs.gl_farm_size.table; delim='\t', append=true, writeheader=true, header=TABLE_COLNAMES)

    format_gl( out, "Region", tabs.gl_gor.table; backend=:typst )
    format_gl( out, "Region - Row %s", gl_to_row_pcts( tabs.gl_gor.table ); backend=:typst, cell_prec=1 )
    println( csvf, "\n\n# Region\n\n")
    CSV.write( csvf, tabs.gl_gor.table; delim='\t', append=true, writeheader=true, header=TABLE_COLNAMES)

    println( out, "#pagebreak()")
    format_gl( out, "Form of Business", tabs.gl_form_of_business.table; backend=:typst )
    format_gl( out, "Form of Business - Row %s", gl_to_row_pcts( tabs.gl_form_of_business.table ); backend=:typst, cell_prec=1 )
    println( csvf, "\n\n# Form of Business\n\n")
    CSV.write( csvf, tabs.gl_form_of_business.table; delim='\t', append=true, writeheader=true, header=TABLE_COLNAMES)

    format_gl( out, "Revenue Quintile (5=highest)", tabs.gl_revenue_quintile.table; backend=:typst )
    format_gl( out, "Revenue Quintile (5=highest) - Row %s", gl_to_row_pcts( tabs.gl_revenue_quintile.table ); backend=:typst, cell_prec=1 )
    println( csvf, "\n\n# Revenue Quintile (5=highest)\n\n")
    CSV.write( csvf, tabs.gl_revenue_quintile.table; delim='\t', append=true, writeheader=true, header=TABLE_COLNAMES)

    format_gl( out, "Outputs Over Inputs Quartile (higher=more efficient)", tabs.gl_outputs_over_inputs_quartile.table; backend=:typst )
    format_gl( out, "Outputs Over Inputs Quartile (higher=more efficient) - Row %s",  gl_to_row_pcts( tabs.gl_outputs_over_inputs_quartile.table ); backend=:typst, cell_prec=1 )
    println( csvf, "\n\n# Outputs Over Inputs Quartile (higher=more efficient)\n\n")
    CSV.write( csvf, tabs.gl_outputs_over_inputs_quartile.table; delim='\t', append=true, writeheader=true, header=TABLE_COLNAMES)

    close(out)

    csvout = open( "$(outname).tab", "w")
    println( csvout, String(take!(csvf)))
    close( csvout )

    typst_command = `typst compile $(outname).typ`
    run( typst_command )
end

function dump_all( outname::String, title::String, summary :: NamedTuple )
    dump_tables_to_typst_and_csv( "$(outname)-fasn-income", "$(title) - % Change in FASN Net Income", summary.gl_net_income )
    dump_tables_to_typst_and_csv( "$(outname)-fasn-revenue", "$(title) - % Change in FASN Farm Revenue", summary.gl_revenue )
    dump_tables_to_typst_and_csv( "$(outname)-fasn-subsidies", "$(title) - % Change in Subsidies, Inc Basic Income", summary.gl_subsidies )
end
