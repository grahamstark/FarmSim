#=
Wrangling the Farm Business Survey Data 
=#

# we've promised to use this only one OneDrive, so....
const DDIR_ONEDRIVE = joinpath( "C:\\","Users","gwdv3","OneDrive - Northumbria University - Production Azure AD","Documents","Basic_Income_Farmers","FarmBusinessSurvey","data" )

# each year unpacked into own directory
const DATADIRS = OrderedDict([
    2021=>joinpath(DDIR_ONEDRIVE, "9041txt", "UKDA-9041-txt", "txt"),
    2022=>joinpath(DDIR_ONEDRIVE, "9287txt", "UKDA-9287-txt", "txt","standard_output_coefficients_2017_version"), # why? who knows ..
    2023=>joinpath(DDIR_ONEDRIVE, "9360txt", "UKDA-9360-txt", "txt")])

"""
Strip £ sign thingy from field_value items, so was can cast as numbers
"""
str2f(s::AbstractString) = parse(Float64,s[3:end])
str2f(s) = s

export open_raw_files, wrangle_datasets
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
ints for each col that supports it.   
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

function categoricalise( d::AbstractDataFrame, calclabels::AbstractDataFrame  )
    # cols in target dataframe that are in calclabels
    targets = intersect(names(d),unique(calclabels.field_name))
    for c in targets 
        # rows of calclabels for the given col in target dataframe
        renrows = calclabels[calclabels.field_name.==c, [:value,:label]]
        # make a set of [val=>label] since that's what the recode function from categorical arrays wants.
        m = Pair[]
        # handle cases in the data but not in calclabels by adding e.g. 42=>"Other-42" and so on.
        unlabelled = setdiff( unique( d[!,c]), renrows.value)
        for u in unlabelled
            if ! ismissing(u)
                push!( m, u=>"Other-$u")
            end
        end
        for r in eachrow(renrows)
            push!(m, r.value=>r.label)
        end
        # recode the col..
        @show c m unlabelled
        d[!,c]=recode(d[!,c],m...)
        # ... and then fix its type as categorical 
    end
    return transform!( d, targets.=>categorical, renamecols=false ), targets
end

function wrangle_datasets( f :: NamedTuple )::NamedTuple
    f.calclabels.field_name = editnames.(f.calclabels.field_name )
    f.calclabels.value = Int.(f.calclabels.value)
    cdw = unstack(f.calcdata,:farm_number,:field_name,:field_value; combine=last )
    to_i!( cdw )
    rename!( editnames, cdw )
    cdw, calcdata_cats=categoricalise( cdw, f.calclabels )
    
    nrows,ncols = size(f.fasdata)
    f.fasdata.v = ones(nrows)
    fdw = unstack( f.fasdata, [:farm_number, :section, :row, :column, :crop_type,:mdc], :v, :field_value; combine=last )
    rename!( fdw, "1.0"=>"field_val")
    to_i!( fdw )
    rename!( editnames, fdw )
    fdw, fasdata_cats=categoricalise( fdw, f.calclabels )
    return ( ; calcdata=cdw, fasdata=fdw, calclabels=f.calclabels, calcdata_cats, fasdata_cats )
end

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

export load_calcdata_as_panel

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
    sort!( calcdata, [:farm_number,:account_year])
    paneldf!(calcdata,:farm_number,:account_year)
    return calcdata
end