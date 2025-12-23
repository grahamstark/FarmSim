#=
Wrangling the Farm Business Survey Data 
=#

"""
Strip £ sign thingy from field_value items, so was can cast as numbers
"""
str2f(s::AbstractString) = parse(Float64,s[3:end])
str2f(s) = s

export 
    load_calcdata_as_panel,
    open_raw_files, 
    wrangle_datasets
    
"""
Open all 4 files for a given year as DataFrames 
"""
function open_raw_files( year::Int )::NamedTuple
    # name like '22calcvars_2017so.txt'
    front, middle = if year == 2021
        "21", ""
    elseif year == 2022
        "22", "_2017so"
    elseif year == 2023
        "23", ""
    end

    #= 
    Files have 1 item per row, no headers, comma delimited. Dumps from MS Access I think.
    Fields names come from the main PDF doc.
    The chain here is because a pipe won't work on its own with extra parameters in the 2nd function (the colname fields).
    =#
    ddr = DATADIRS[year]
    calcdata = @chain begin
        CSV.File( joinpath( ddr, "$(front)calcdata$(middle)_protect.txt"); delim=',', header=false )
        DataFrame(_, [:farm_number, :sort_order, :load_number, :field_name, :field_value])
    end
    calcdata.field_value = str2f.( calcdata.field_value )
    calcvars = @chain begin
        CSV.File( joinpath( ddr, "$(front)calcvars$(middle)_protect.txt" ); delim=',', header=false )
        DataFrame( _, [:id,:id2,:field,:formula])
    end
    fasdata = @chain begin
        CSV.File( joinpath( ddr, "$(front)fasdata$(middle)_protect.txt" ); delim=',', header=false )
        DataFrame( _,[ :farm_number, :section, :row, :column, :crop_type, :mdc, :load_number, :field_value])
    end 
    fasdata.field_value = str2f.( fasdata.field_value )
    calclabels = @chain begin 
        CSV.File( joinpath( ddr, "$(front)calclabels$(middle)_protect.txt" ); delim=',', header=false )
        DataFrame( _, [:field_name, :value, :label])
    end 
    return (; calcvars, calcdata, fasdata, calclabels )
end

function editnames(s::AbstractString)::AbstractString
    lowercase( replace(s,"."=>"_"))
end

"""
Convert each col from strings to ints for each col that supports it.   
"""
function to_i!( d::AbstractDataFrame )
    nrows,ncols = size(d)
    for c in 1:ncols
        try
            d[!,c] = Int.(d[!,c])
        catch e
          ;
        end
    end
end 

"""
Change fields in `d` from ints to one of the labels in the `calclabels` DataFrame

return transformed dataframe and a list of cols that have been edited.
"""
function categoricalise( d::AbstractDataFrame, calclabels::AbstractDataFrame  )::Tuple
    # cols in target dataframe (wide) that are in calclabels (long)
    targets = intersect(names(d),unique(calclabels.field_name))
    for c in targets 
        # rows of calclabels for the given col in target dataframe
        renrows = calclabels[calclabels.field_name.==c, [:value,:label]]
        # make a set of [val=>label] since that's what the recode function from categorical arrays wants.
        val_to_label_maps = Pair[]
        # handle cases in the data but not in calclabels by adding e.g. 42=>"Other-42" and so on.
        unlabelled = setdiff( unique( d[!,c]), renrows.value)
        for u in unlabelled
            if ! ismissing(u)
                push!( val_to_label_maps, u=>"Other-$u")
            end
        end
        for r in eachrow(renrows)
            push!(val_to_label_maps, r.value=>r.label)
        end
        # recode the col..
        @show c m unlabelled
        d[!,c]=recode(d[!,c],val_to_label_maps...)
        # ... and then fix its type as categorical 
    end
    return transform!( d, targets.=>categorical, renamecols=false ), targets
end

"""
`f`: a collection of datasets for some data year.
-  turn the main ones from long->wide 
-  then convert int fields to string categories
"""
function wrangle_datasets( f :: NamedTuple )::NamedTuple
    f.calclabels.field_name = editnames.(f.calclabels.field_name )
    f.calclabels.value = Int.(f.calclabels.value)
    cdw = unstack(f.calcdata,:farm_number,:field_name,:field_value; combine=last )
    to_i!( cdw )
    rename!( editnames, cdw )
    cdw, calcdata_cats=categoricalise( cdw, f.calclabels )
    
    nrows,ncols = size(f.fasdata)
    # f.fasdata.v = ones(nrows)
    
    f.fasdata.vname = f.fasdata.section .* "-" .* string.(f.fasdata.row)
    fdw = unstack( fas, :farm_number, :vname, :field_val, combine=last )
    # fdw = unstack( f.fasdata, [:farm_number, :section, :row, :column, :crop_type,:mdc], :v, :field_value; combine=last )
    # rename!( fdw, "1.0"=>"field_val")
    to_i!( fdw )
    rename!( editnames, fdw )
    fdw, fasdata_cats=categoricalise( fdw, f.calclabels )
    fdw = innerjoin!( cdw, fdw ; on=:farm_number )
    return ( ; calcdata=cdw, fasdata=fdw, calclabels=f.calclabels, calcdata_cats, fasdata_cats )
end

export to_i!

# int formatter with 3 fixed places: 
# 9=>009, 19=>019, 119=>119 so sort by varnames goes right way
zfm(i)=format(i; width=3, zeropadding=true)
#
# hack one-off version for the raw data. Assumes calcdata already created
# returns merged 1 year calc and raw data, and two ordered sets of varnames 
# that can be used
# to get everything back in order after merging years, with calc then raw.
#
function make_combined_hack( year :: Int )::Tuple
    fass = CSV.File( "$(DIR)fasdata-2023.tab")|>DataFrame
    fass.vname = lowercase.(fass.section) .* "_" .* zfm.(fass.row)
    fass = unstack( fass, :farm_number, :vname, :field_val, combine=last ) 
    fass = coalesce.( fass, 0.0 )
    to_i!( fass )
    n2 = sort(names(fass))
    cdw=CSV.File( "$(DIR)/calcdata-$(year).tab")|>DataFrame
    n1 = names(cdw)
    fass = innerjoin( cdw, fass; on=:farm_number )
    # needed for keeping in order
    return fass, OrderedSet(n1),OrderedSet(n2)
end

#
# Hack creation of panel 
#
function make_panel_hack()
    calcdata,n1,n2 = make_combined_hack( 2021 )
    for year in 2022:2023
        d,nd1,nd2 = make_combined_hack( year )
        append!(calcdata,d; cols=:union)
        n1 = union( n1, nd1 )
        n2 = union( n2, nd2 )
    end
    nn = unique( vcat( collect(n1), collect(n2)))    
    calcdata = coalesce.( calcdata, 0.0 )
    select!(calcdata,nn)
    # this adds counts of years in the panel 
    farms = groupby( calcdata,:farm_number)
    panelsize=combine( farms,(:farm_number=>length), (:account_year=>minimum), (:account_year=>maximum))
    calcdata = outerjoin( calcdata, panelsize;on=:farm_number )
    sort!( calcdata, [:farm_number_length, :farm_number, :account_year]; rev=true)
    # I think you can do this next one in the `combine` function
    rename!( calcdata, [:farm_number_length=>:num_years, :account_year_minimum=>:first_panel_year,:account_year_maximum=>:last_panel_year])
    # cast into a Panel DataFrame - FIXME: actually does nothing ...
    calcdata = coalesce.( calcdata, 0.0 )
    paneldf!( calcdata,:farm_number,:account_year)
    CSV.write( "$(DIR)/joined-raw-data-2021-2023.tab", calcdata; delim='\t')
    return calcdata
end


"""
For each year, create wide,categorialised datasets in their own directories.
"""
function create_and_save( )
    editdir = joinpath(FarmSim.DDIR_ONEDRIVE,"edited")
    try
        mkdir(editdir)
    catch
        ;
    end
    for year in 2021:2023
        println("on year $year")
        fr = open_raw_files(year)
        f = wrangle_datasets(fr)
        open(joinpath(editdir,"calcdata-cats-$(year).txt"),"w") do io
            println( io, join(f.calcdata_cats,"\n" ))
        end
        open(joinpath(editdir,"fasdata-cats-$(year).txt"),"w") do io
            println( io, join(f.fasdata_cats,"\n" ))
        end
        CSV.write( joinpath(editdir,"calcdata-$(year).tab"), f.calcdata; delim='\t' )
        CSV.write( joinpath(editdir,"fasdata-$(year).tab"), f.fasdata; delim='\t' )
    end
end

"""
Load all 3 waves of the calcdata into a single frame, sorted by `farm_number`.
Add counts of years,1st and final year, for each farm 
"""
function load_calcdata_as_panel()::AbstractDataFrame
    
    function readone( editdir, year  )::DataFrame
        d = CSV.File( joinpath( editdir, "calcdata-$(year).tab"))|>DataFrame
        cats = readlines( joinpath( editdir, "calcdata-cats-$(year).txt"))
        return transform!( d, cats.=>categorical, renamecols=false )
    end

    editdir = joinpath(FarmSim.DDIR_ONEDRIVE,"edited")
    calcdata = readone( editdir, 2021 )
    for year in 2022:2023
        d = readone( editdir, year )
        append!(calcdata,d; cols=:union)
    end
    # this adds counts of years in the panel 
    farms = groupby( calcdata,:farm_number)
    panelsize=combine( farms,(:farm_number=>length), (:account_year=>minimum), (:account_year=>maximum))
    calcdata = outerjoin( calcdata, panelsize;on=:farm_number )
    sort!( calcdata, [:farm_number_length, :farm_number, :account_year])
    # I think you can do this next one in the `combine` function
    rename!( calcdata, [:farm_number_length=>:num_years, :account_year_minimum=>:first_panel_year,:account_year_maximum=>:last_panel_year])
    # cast into a Panel DataFrame - FIXME: actually does nothing ...
    paneldf!( calcdata,:farm_number,:account_year)
    CSV.write( COMBINED_CALCDATA, calcdata; delim='\t')
    return calcdata
end

"""
create crude unweighted growth rates for the numerical cols  
"""
function find_growth_rates( calcdata )
    nrows,ncols = size(calcdata)
    nms = names(calcdata)
    outdata = DataFrame( col = fill("",nrows), mean_2021=fill(0.0, nrows), mean_2022=fill(0.0, nrows), mean_2023=fill(0.0, nrows))
    row = 0
    for year in 2021:2023
        row = 0
        ydata = calcdata[(calcdata.num_years .== 3) .& (calcdata.account_year .== year),:] 
        # w = Weights( ydata.weight )
        for n in nms 
            c = ydata[!,n]
            v = collect(skipmissing(c))
            if eltype(v) <: Union{Missing,AbstractFloat}
                row += 1
                # av = mean( c, w )
                outdata[row,:col] = n
                t = Symbol( "mean_$year")
                outdata[row,t] = mean( v )
            end
        end
    end
    outdata.avgr_2021_22 = (outdata.mean_2022./outdata.mean_2021) .- 1
    outdata.avgr_2022_23 = (outdata.mean_2023./outdata.mean_2022) .- 1
    return outdata[1:row,:]
end # find_growth_rates