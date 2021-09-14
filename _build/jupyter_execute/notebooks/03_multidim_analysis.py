#!/usr/bin/env python
# coding: utf-8

# ## Multi-dimensional Analysis with xarray
# 
# ### Questions
# - What Python tools can I use work with multidimensional data like NetCDF files? 
# 
# ### Objectives
# - Learn how to use xarray to conscisely work with multidimensional data

# ###  Introduction to multidimensional arrays
# 
# Unlabelled, N-dimensional arrays of numbers, such as NumPy's ndarray, are the most widely used data structure in scientific computing. Geoscientists have a particular need for structuring their data as arrays. For example, we commonly work with sets of climate variables (e.g. temperature and precipitation) that vary in space and time and are represented on a regularly-spaced grid. Often we need to subset a large global grid to look at data for a particular region, or select a specific time slice. Then we might want to apply statistical functions to these subsetted groups to generate summary information.
# 
# <br>
# <img src="http://xarray.pydata.org/en/stable/_images/dataset-diagram.png" width = "800" border = "10">
# <br>
# 
# > ## Isn't this the same as raster processing?
# > The tools in this tutorial have some similarity to raster image processing tools.
# > Both require computational engines that can manipulate large stacks of data formatted as arrays.
# > Here we focus on tools that are optimized to handle data that have many variables spanning dimensions
# > of time and space. See the raster tutorials for tools that are optimized for image processing of remote sensing datasets.
# 
# 
# ### Conventional Approach: Working with Unlabelled Arrays
# 
# Multidimensional array data are often stored in user-defined binary formats, and distributed with custom Fortran
# or C++ libraries used to read and process the data. Users are responsible for setting up their own file structures and custom codes to handle these files. Subsetting the data involves reading everything into an in-memory array, and then using a series of nested loops with conditional statements to look for a specific range of index values associated with the temporal or spatial slice needed. Also, clever use of matrix algebra is often used to summarize data across spatial and temporal dimensions.
# 
# ### Challenges:
# 
# The biggest challenge in working with N-dimensional arrays in this fashion is the fact that the data are almost disassociated from their metadata. Users are left with the task of tracking the meaning behind array indices using domain-specific software, often leading to inefficiencies and errors. Common pitfalls often occur in in the form of questions like "is the time axis of my array in the first or third index position?", or "does my array of timestamps still align with my data after resampling?".

# In[1]:


import os
import json
import rasterio
import requests

import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[ ]:


import xarray as xr 


# **Recall that we are interested in looking at landuse over the State of Pará in Brazil, where extensive logging and illegal deforestation is happening. The Landsat tile we will be looking at is Path 227, Row 065. The date for the file we will be accessing is 8 June, 2020 and we will extract the NIR, red band and metadata file from the AWS s3 bucket**
# 

# In[ ]:


# Open path to file on s3 bucket with rasterio
print('Landsat on AWS:')
filepath = 'http://landsat-pds.s3.amazonaws.com/c1/L8/227/065/LC08_L1TP_227065_20200608_20200626_01_T1/LC08_L1TP_227065_20200608_20200626_01_T1_B4.TIF'
with rasterio.open(filepath) as src:
    print(src.profile)


# In[ ]:


date = '2020-06-08'
url = 'http://landsat-pds.s3.amazonaws.com/c1/L8/227/065/LC08_L1TP_227065_20200608_20200626_01_T1/'
redband = 'LC08_L1TP_227065_20200608_20200626_01_T1_B{}.TIF'.format(4)
nirband = 'LC08_L1TP_227065_20200608_20200626_01_T1_B{}.TIF'.format(5)
mtlfile = 'LC08_L1TP_227065_20200608_20200626_01_T1_{}.json'.format('MTL')

# Overviews are reduced resolution versions of your dataset that can speed up rendering when you don’t need 
# full resolution. By precomputing the upsampled pixels, rendering can be significantly faster when zoomed out.
# More info here: https://rasterio.readthedocs.io/en/latest/topics/overviews.html

with rasterio.open(url+redband) as src:
    profile = src.profile
    oviews = src.overviews(1) # list of overviews from biggest to smallest
    oview = oviews[1]  # Use second-highest resolution overview
    print('Decimation factor= {}'.format(oview))
    red = src.read(1, out_shape=(1, int(src.height // oview), int(src.width // oview)))


# In[ ]:


# Let's look at what the red band image looks like

plt.imshow(red)
plt.colorbar()
plt.title('{}\nRed {}'.format(redband, red.shape))
plt.xlabel('Column #')
plt.ylabel('Row #')


# In[ ]:


# Let's download the files 
def download_file(in_filename, out_filename):
    if not os.path.exists(out_filename):
        print("Downloading", in_filename)
        response = requests.get(in_filename)
        with open(out_filename, 'wb') as f:
            f.write(response.content)


# In[ ]:


nir_filename = url + nirband
red_filename = url + redband
mtl_filename = url + mtlfile

download_file(nir_filename, 'nir.tif')
download_file(red_filename, 'red.tif')
download_file(mtl_filename, 'meta.json')


# In[ ]:


# Get the shape size for the red band image

red = rasterio.open(red_filename)
print(red.is_tiled)
red.block_shapes


# In[ ]:


# Technically you do not need to download the files to read the data

red = xr.open_rasterio(red_filename, chunks={'band': 1, 'x': 1024, 'y': 1024})
nir = xr.open_rasterio(nir_filename, chunks={'band': 1, 'x': 1024, 'y': 1024})
red


# ####  <font color=blue>Exercise 1: Inspect the dimensions in the dataset. How would you only view the dimensions along the x dimension? </font>

# In[ ]:


# Answer for Exercise 1


# ### Calculate and plot NDVI

# The Landsat Level 1 images are delivered in a quantized format. This has to be converted to top-of-atmosphere reflectance using the provided metadata.
# 
# First we define convenience functions to load the rescaling factors and transform a dataset. The red band is band 4 and near infrared is band 5.
# 
# We will also introduce here the concept of Dask, a flexible library for parallel computing in Python

# N𝐷𝑉𝐼=𝑁𝐼𝑅−𝑅𝑒𝑑 / 𝑁𝐼𝑅+𝑅𝑒𝑑

# In[ ]:


import dask
from dask.distributed import Client
client = Client(processes=False)
client


# In[ ]:


def load_scale_factors(filename, band_number):
    with open(filename) as f:
        metadata = json.load(f)
    M_p = metadata['L1_METADATA_FILE']                   ['RADIOMETRIC_RESCALING']                   ['REFLECTANCE_MULT_BAND_{}'.format(band_number)]
    A_p = metadata['L1_METADATA_FILE']                   ['RADIOMETRIC_RESCALING']                   ['REFLECTANCE_ADD_BAND_{}'.format(band_number)]
    return M_p, A_p


# In[ ]:


def calculate_reflectance(ds, band_number, metafile='meta.json'):
    M_p, A_p = load_scale_factors(metafile, band_number)
    toa = M_p * ds + A_p
    return toa


# In[ ]:


red_toa = calculate_reflectance(red, band_number=4)
nir_toa = calculate_reflectance(nir, band_number=5)


# In[ ]:


print(red_toa.variable.data)


# In[ ]:


red_max, red_min, red_mean = dask.compute(
    red_toa.max(dim=['x', 'y']),
    red_toa.min(dim=['x', 'y']),
    red_toa.mean(dim=['x', 'y'])
)
print(red_max.item())
print(red_min.item())
print(red_mean.item())


# In[ ]:


ndvi = (nir_toa - red_toa) / (nir_toa + red_toa)


# In[ ]:


ndvi2d = ndvi.squeeze()


# In[ ]:


plt.figure()
im = ndvi2d.compute().plot.imshow(cmap='BrBG', vmin=-0.5, vmax=1)
plt.axis('equal')
plt.show()

