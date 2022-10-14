# List of instance ids to bring to process
# Note that the version is for now ignored
# (the latest is always chosen) TODO: See if
# we can make this specific to the version
import asyncio

from pangeo_forge_esgf import generate_recipe_inputs_from_iids

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


# Figure out which activity each experiment belongs to
def get_MIP(experiment):
    if experiment == 'ssp245-covid':
        return 'DAMIP'
    elif experiment == 'ssp370-lowNTCF':
        return 'AerChemMIP'
    elif experiment.startswith('ssp'):
        return 'ScenarioMIP'
    elif experiment.startswith('hist-'):
        return 'DAMIP'
    else:
        return 'CMIP'


model = 'NorESM2-LM'
# experiments = [
#                '1pctCO2', 'abrupt-4xCO2', 'historical', 'piControl', # CMIP
#                'hist-GHG', 'hist-aer', # DAMIP
#                'ssp126', 'ssp245', 'ssp370', 'ssp370-lowNTCF', 'ssp585' #	ScenarioMIP
# ]
# Test with a subset
experiments = ['1pctCO2', 'hist-GHG', 'ssp126']
variables = ['tas', 'tasmin', 'tasmax', 'pr']

# Construct the  required list of IIDS
climatebench_iids = []
for variable in variables:
    for experiment in experiments:
        physics = 2 if experiment == 'ssp245-covid' else 1  # The COVID simulation uses a different physics setup
        for i in range(3):
            ensemble_member = f"r{i+1}i1p1f{physics}"
            climatebench_iids.append(f'CMIP6.{get_MIP(experiment)}.NCC.NorESM2-LM.{experiment}.{ensemble_member}.day.{variable}.gn')

# Build the recipe inputs
recipe_inputs = asyncio.run(generate_recipe_inputs_from_iids(climatebench_iids))

recipes = {}


def annual_mean(ds):
    return ds.groupby('time.year').mean('time')


def annual_extreme(ds, quantile=0.9):
    return ds.groupby('time.year').quantile(quantile, skipna=True)


for iid, recipe_input in recipe_inputs.items():
    urls = recipe_input.get('urls', None)
    pattern_kwargs = recipe_input.get('pattern_kwargs', {})
    recipe_kwargs = recipe_input.get('recipe_kwargs', {})

    pattern = pattern_from_file_sequence(urls, 'time', **pattern_kwargs)

    recipe_key = iid.replace(".day.", '.ann.')
    if urls is not None:
        recipes[recipe_key] = XarrayZarrRecipe(
            pattern, xarray_concat_kwargs={'join': 'exact'}, process_chunk=annual_mean, **recipe_kwargs
        )
        # If it's precip then do the 90th percentile too...
        if ".pr." in recipe_key:
            recipes[recipe_key.replace(".pr.", ".pr90.")] = XarrayZarrRecipe(
                pattern, xarray_concat_kwargs={'join': 'exact'}, process_chunk=annual_extreme, **recipe_kwargs
            )

# The iids changed so check against the correct ones
climatebench_iids_to_check = [iid.replace(".day.", ".ann.") for iid in climatebench_iids]
print('+++Failed iids+++')
print(list(set(climatebench_iids_to_check) - set(recipes.keys())))
print('+++Successful iids+++')
print(list(recipes.keys()))
