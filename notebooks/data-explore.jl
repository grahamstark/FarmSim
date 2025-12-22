### A Pluto.jl notebook ###
# v0.20.21

using Markdown
using InteractiveUtils

# ╔═╡ 9817d960-dc29-11f0-ad5f-3f05e52e8af6
using CSV, DataFrames, StatsBase,CairoMakie, Format

# ╔═╡ 9dac990d-4f79-49f1-a9dd-d9a8502bee66
md""" 

# Farm Business Survey 

### Data Exploration

> The Farm Business Survey was initiated in 1936 as the Farm Management Survey with the objective of systematically collecting, for the first time, information on the economic condition of farming in England and Wales. The objectives of the Survey were and still are "to make available, year by year, such information as would provide a statistical basis for the study of the economic problems of the industry .... To provide a useful indication of the level of farm incomes each year and, over a series of years, to indicate the general trend, thereby enabling more reliable judgements on these matters to be formed.

From [FARM BUSINESS SURVEY Collection Instructions](https://assets.publishing.service.gov.uk/media/5ee0cc9dd3bf7f1eb2043f19/fbs-instructions-201920_11jun20.pdf)

"""

# ╔═╡ 085a1619-2929-4394-8b1e-d3d2048d1e83


# ╔═╡ cb970315-80a4-4c52-aa41-21556c3d109d
const PATH="/mnt/data/fadn/";

# ╔═╡ 8b7a255f-e136-4ad2-952c-a13b5d30cb4b
begin
	adm= CSV.File( joinpath( PATH, "calcdata-2021-2023-combined.tab"))|>DataFrame
	adm = coalesce.(adm,0)
	adm.weight=Weights(adm.weight)
	byyear = groupby( adm, :account_year )
	
	const NAMES = names(adm)
	
	
function namesearch( key )	
	matches=[]
	re = Regex( "(.*$(key).*)")
	for i in NAMES
		m = match(re,i)
		if ! isnothing(m) 
			push!(matches,Symbol(m[1]))
		end
	end
	sort(matches)
end

# grouped names of variables
const INCOME = namesearch( "income" )
const WAGE=namesearch( "wage" )
const SUBSIDIES = [
    :subsidies, 
    :fadn_current_subsidies_taxes, 
    :other_environment_grants_and_subsidies, 
    :non_crop_livestock_grants_subsidies, 
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
    :working_spouse, 
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
    :other_unpaid_workers ]

end;

# ╔═╡ 9b246e60-adb7-4707-b820-ccfdc6966d69
adm23 =  CSV.File( joinpath( PATH, "calcdata-2023.tab"))|>DataFrame

# ╔═╡ 0c5fb70c-348f-4fd2-98a5-af0dcfaf0e6a
valuation_change_crops_livestock = adm.valuation_change_crops +adm.valuation_change_livestock 

# ╔═╡ 61da25df-837c-48ab-8891-c1d9c17a601e
mean( byyear[3].farm_business_income_incl_blsa, Weights(byyear[3].weight ))

# ╔═╡ 69177fb1-0a3a-4432-acbc-843b6eb59f96
sum(valuation_change_crops_livestock - adm.valuation_change_crops_livestock)

# ╔═╡ e9182342-7db9-40bf-8804-adb123110239
rop_subsidies = adm.area_payments +adm.set_aside_payments +adm.other_crop_subsidies 

# ╔═╡ 435b1a10-9c12-412e-890b-3dd41bfa750f
other_livestock_subsidies = adm.livestock_subsidies -adm.scp_payments -adm.bsp_payments -adm.sap_payments -adm.bull_slaughter_premium 

# ╔═╡ de372578-44c7-46f8-91ce-482476490e96
adm.bsp

# ╔═╡ c318081b-9013-425b-a299-aee138797c73
byyear[3][!,INCOME]

# ╔═╡ 48cd83ec-d631-43fe-b651-53ecd9805e8e
begin
	farm_business_income_d	= adm.farm_business_output - adm.farm_business_costs + adm.farm_business_tenant_capital_sale_profits
	farm_business_income_d ≈ adm.farm_business_income
end

# ╔═╡ cabc1675-1178-4e62-ae99-b0e7fc7ddf40
begin
farm_business_output_d=adm.crop_output_excl_subsidies + adm.livestock_output_excl_subsidies + adm.output_subsidies + adm.sectioni_output
farm_business_output_d ≈ adm.farm_business_output
end

# ╔═╡ f7fcf3c8-89c0-453c-8065-0bb51422193b
adm.epub_farm_type

# ╔═╡ b145f67f-a973-42e8-9513-0aadf8ce8a6f
md"""
The below is not quite table 3.1a from [Chapter 3: Farming income](https://www.gov.uk/government/statistics/agriculture-in-the-united-kingdom-2023/chapter-3-farming-income#distribution-of-farm-incomes-and-performance). I think the problem is that there's a `Horticulture` in the data but not in table 3.1a.
"""

# ╔═╡ 430272db-3d67-4e7c-8b14-3d4de07b59ed
begin
	
wmean(x,y) = format(round(mean(x,Weights(y))/500.0)*500;commas=true, precision=0)
	
function table_3( 
	adm::AbstractDataFrame;
	income::Symbol, 
	breakdown=:farm_type,
	weight=:weight )::AbstractDataFrame
	ghh = combine(groupby( adm, [:account_year, breakdown] ),([income,weight]=>wmean=>:income))
	sort!( ghh, :account_year)
	vhh = unstack( ghh, :account_year, :income )
end
	
adm.farm_business_income_non_neg = max.(adm.farm_business_income,(0.0,))

# try this every way ..
table_3(adm; income=:fadn_gross_farm_income )
table_3(adm; income=:farm_business_income, breakdown=:epub_farm_type )
table_3(adm; income=:farm_business_income,weight=:weight_uncalibrated )
table_3(adm; income=:farm_business_income_incl_blsa )
table_3(adm; income=:farm_business_income_non_neg )
table_3(adm; income=:farm_business_income )

end

# ╔═╡ e323c4b8-0c90-46b3-bfbb-77885d2e801d
md"""

There are some obvious bugs in the data, for example the `subsidies` column should be the sum of all subsidies from all sources (see the `calcvars` file), but it clearly isn't:

"""

# ╔═╡ 33fb1c96-310c-471a-a4eb-a0f85030f32b
md"""

## Acronyms 

[Glossary](https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Category:Glossary)

* **FADN** Farm Accountancy Data Network (now FSDN - Farm sustainability data network)
* **AWU** Annual Work Unit - the full-time equivalent employment, i.e. the total hours worked divided by the average annual hours worked in full-time jobs
* **ALU**
* **LFA** less favoured area (LFA);
* **UAA** Utilised agricultural area (UAA) the agricultural area of the farm;
* **SO** Standard Output - SOs represent the level of output that could be expected on the average farm under “normal” conditions (i.e. no disease outbreaks or adverse weather). 
* **BLSA** Breeding Livestock Stock Appreciation. Breeding livestock stock appreciation represents the change in market prices of breeding cattle, sheep and pigs between the opening and closing valuations. It is not included in the calculation of farm business income. 
* **Net Farm Income** Net Farm Income is a narrower measure of income; it is net of an imputed rent on owned land and an imputed cost for unpaid labour (apart from farmer and spouse). On this basis a quarter of farms in Great Britain failed to make a profit.
* **Farm business income** (FBI) for sole traders and partnerships represents the financial return to all unpaid labour (farmers and spouses, non-principal partners and directors and their spouses and family workers) and on all their capital invested in the farm business, including land and buildings. For corporate businesses it represents the financial return on the shareholders capital invested in the farm business. 

See also the income definitions in [Definitions used by the Farm Business Survey](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/557605/fbs-definintions-4oct16.pdf) and [Ag In UK 2023](https://www.gov.uk/government/statistics/agriculture-in-the-united-kingdom-2023/chapter-3-farming-income)*

"""

# ╔═╡ c1d3ea09-a39d-48d7-9637-bb778d4d408b
md"""

## SOURCES

* [FBS Technical Notes](https://www.gov.uk/guidance/farm-business-survey-technical-notes-and-guidance#technical-notes);
* [FBS Definitions](https://assets.publishing.service.gov.uk/media/5a80f39ce5274a2e87dbcbd9/fbs-definintions-4oct16.pdf);
* [FBS Weighting](https://www.gov.uk/government/publications/calibration-weighting-for-the-farm-business-survey-fbs-in-england-november-2025-update/calibration-weighting-for-the-farm-business-survey-fbs-in-england-november-2025-update);
* [Summary of 2019-2021 FBS](https://assets.publishing.service.gov.uk/media/65c09f30c4319100141a44fd/FBS_Evidence_Pack_24jan24i.pdf);
* [Agriculural Workforce](https://www.gov.uk/government/statistics/agricultural-workforce-in-england-at-1-june/agricultural-workforce-in-england-at-1-june-2025);
* ...from the [Survey of Agriculture and Horticulture](https://www.gov.uk/guidance/structure-of-the-agricultural-industry-survey-notes-and-guidance#june-survey-of-agriculture-and-horticulture-in-england);
* [Agriculture in the United Kingdom](https://www.gov.uk/government/statistics/agriculture-in-the-united-kingdom-2023) (based on FBS)
* [FARM BUSINESS SURVEY Collection Instructions](https://assets.publishing.service.gov.uk/media/5ee0cc9dd3bf7f1eb2043f19/fbs-instructions-201920_11jun20.pdf)

"""

# ╔═╡ 27391e63-45a3-4459-b89f-756d12c43252


# ╔═╡ 87e05af5-a1ad-4d6e-8a68-3dcbc7c2227e


# ╔═╡ 03cf8bb9-40f6-458d-a101-fa1707c21bac
byyear[1][!,SUBSIDIES]

# ╔═╡ 0c542e88-6a17-4318-851c-1ecbd252fac0
adm.fadn_current_subsidies_taxes - (adm.general_farm_subsidies_environment_payments)

# ╔═╡ 88026c04-6f81-4e28-b060-5a1fc00653e4
begin
	x=[]
	for i in byyear[1][7,:]
		if(typeof(i) <: Number) && (i ≈ 1843.0)
			push!(x,i)
		end
	end
	x
end

# ╔═╡ f05af3e4-75d1-4e67-a60d-fcd9aa3c77ea
byyear[3]

# ╔═╡ 6e682e09-76e2-40a4-9160-ed315c7eb823
begin
	sums = []
	for year in 2021:2023
		a = adm[adm.account_year .== year,:]
		push!( sums, (;
			year,
			total_workers = a.weight' *( a.paid_workers + a.unpaid_workers),
			total_subsidies = (a.weight' * a.fadn_current_subsidies_taxes)/1_000_000))
	end
	sums
end

# ╔═╡ 8a1cadf5-dd27-43f5-8b3f-5272853e1de6
WORKERS

# ╔═╡ 9d837133-615b-4daa-8625-4d9233a4ad13
adm.unpaid_workers

# ╔═╡ f84c91bf-f850-43c7-a15b-0dda34b24578
byyear[( 2023, )]

# ╔═╡ Cell order:
# ╠═9817d960-dc29-11f0-ad5f-3f05e52e8af6
# ╟─9dac990d-4f79-49f1-a9dd-d9a8502bee66
# ╠═085a1619-2929-4394-8b1e-d3d2048d1e83
# ╠═cb970315-80a4-4c52-aa41-21556c3d109d
# ╠═8b7a255f-e136-4ad2-952c-a13b5d30cb4b
# ╠═9b246e60-adb7-4707-b820-ccfdc6966d69
# ╠═0c5fb70c-348f-4fd2-98a5-af0dcfaf0e6a
# ╠═61da25df-837c-48ab-8891-c1d9c17a601e
# ╠═69177fb1-0a3a-4432-acbc-843b6eb59f96
# ╠═e9182342-7db9-40bf-8804-adb123110239
# ╠═435b1a10-9c12-412e-890b-3dd41bfa750f
# ╠═de372578-44c7-46f8-91ce-482476490e96
# ╠═c318081b-9013-425b-a299-aee138797c73
# ╠═48cd83ec-d631-43fe-b651-53ecd9805e8e
# ╠═cabc1675-1178-4e62-ae99-b0e7fc7ddf40
# ╠═f7fcf3c8-89c0-453c-8065-0bb51422193b
# ╟─b145f67f-a973-42e8-9513-0aadf8ce8a6f
# ╠═430272db-3d67-4e7c-8b14-3d4de07b59ed
# ╠═e323c4b8-0c90-46b3-bfbb-77885d2e801d
# ╟─33fb1c96-310c-471a-a4eb-a0f85030f32b
# ╟─c1d3ea09-a39d-48d7-9637-bb778d4d408b
# ╠═27391e63-45a3-4459-b89f-756d12c43252
# ╠═87e05af5-a1ad-4d6e-8a68-3dcbc7c2227e
# ╠═03cf8bb9-40f6-458d-a101-fa1707c21bac
# ╠═0c542e88-6a17-4318-851c-1ecbd252fac0
# ╟─88026c04-6f81-4e28-b060-5a1fc00653e4
# ╠═f05af3e4-75d1-4e67-a60d-fcd9aa3c77ea
# ╠═6e682e09-76e2-40a4-9160-ed315c7eb823
# ╠═8a1cadf5-dd27-43f5-8b3f-5272853e1de6
# ╠═9d837133-615b-4daa-8625-4d9233a4ad13
# ╠═f84c91bf-f850-43c7-a15b-0dda34b24578
