#!/usr/bin/env python
# coding: utf-8

# # Goal of this notebook is to scale through a cluster to calculate NDVI across 128+ Landsat Scenes (using DASK)

# (https://landsat.usgs.gov/landsat_acq)
# Use the tools on this page to determine when the Landsat 7 and Landsat 8 satellites acquire data over your area of interest, and to view the paths that were acquired on any given day. 
# 
# Landsat satellites image the entire Earth every 16 days in an 8-day offset. Landsat 7 acquires data in descending (daytime) node, while Landsat 8 acquires data in both descending and occasional ascending (nighttime) node. 

# 

# The Landsat 8 satellite orbits the the Earth in a sun-synchronous, near-polar orbit, at an altitude of 705 km (438 mi), inclined at 98.2 degrees, and circles the Earth every 99 minutes. The satellite has a 16-day repeat cycle with an equatorial crossing time: 10:00 a.m. +/- 15 minutes.
# 
# Landsat satellites image the entire Earth every 16 days in an 8-day offset. Landsat 7 acquires data in descending (daytime) node, while Landsat 8 acquires data in both descending and occasional ascending (nighttime) node. 
# 
# Row refers to the latitudinal center line of a frame of imagery. As the satellite moves along its path, the observatory instruments are continuously scanning the terrain below.  These will be squares centered on the orbital path, but tilted clockwise when views on the UTM projection used for the distributed data.

# In[ ]:





# In[1]:


url = 'http://landsat-pds.s3.amazonaws.com/c1/L8/227/065/LC08_L1TP_227065_20200608_20200626_01_T1/'
redband = url+'LC08_L1TP_227065_20200608_20200626_01_T1_B{}.TIF'.format(4)


# In[2]:


get_ipython().system('wget {redband}')


# In[3]:


get_ipython().system('gdalinfo LC08_L1TP_227065_20200608_20200626_01_T1_B4.TIF')


# In[4]:


import gdal
from gdalconst import GA_ReadOnly

data = gdal.Open('LC08_L1TP_227065_20200608_20200626_01_T1_B4.TIF', GA_ReadOnly)
geoTransform = data.GetGeoTransform()
minx = geoTransform[0]
maxy = geoTransform[3]
maxx = minx + geoTransform[1] * data.RasterXSize
miny = maxy + geoTransform[5] * data.RasterYSize
print ([minx, miny, maxx, maxy])
data = None


# EPSG:4326
# WGS 84 -- WGS84 - World Geodetic System 1984, used in GPS

# Reproject "EPSG",32621 to EPSG:4326

# Lets find the bounding box for the region

# In[6]:


from osgeo import gdal

input_raster=gdal.Open('LC08_L1TP_227065_20200608_20200626_01_T1_B4.TIF')
output_raster = "Reproj" + 'LC08_L1TP_227065_20200608_20200626_01_T1_B4.TIF'
gdal.Warp(output_raster,input_raster,dstSRS='EPSG:4326')


# In[7]:


data = gdal.Open('ReprojLC08_L1TP_227065_20200608_20200626_01_T1_B4.TIF', GA_ReadOnly)
geoTransform = data.GetGeoTransform()
minx = geoTransform[0]
maxy = geoTransform[3]
maxx = minx + geoTransform[1] * data.RasterXSize
miny = maxy + geoTransform[5] * data.RasterYSize
print ([minx, miny, maxx, maxy])
data = None


# If you have trouble getting the right scene to cover the area you want:
# 
# Go to https://landsat.usgs.gov/wrs-2-pathrow-latitudelongitude-converter  and enter the lat/long. 
# Then note the path row, and when you get the Landsat data, insure you have the correct path and row, which are listed in the download table.
# Use https://landsat.usgs.gov/landsat_acq tool, which will show the coverage for each data
# Path/Row shapefiles and KML: https://www.usgs.gov/land-resources/nli/landsat/landsat-shapefiles-and-kml-files
# KML file direct download:https://prd-wret.s3-us-west-2.amazonaws.com/assets/palladium/production/atoms/files/WRS-2_bound_world_0.kml

# In[9]:


#get list of images
import requests
from bs4 import BeautifulSoup
import re
URL = 'https://landsatonaws.com/L8/227/065/'
page = requests.get(URL)

soup = BeautifulSoup(page.content, 'html.parser')


# In[10]:


# Use beautiful soup to extract all the links, and Tier 1 only
LevelOnes=[]
for link in soup.findAll('a', attrs={'href': re.compile("^/L8/227/065")}):
    #print(link.get('href'))
    href = link.get('href')
    if href.endswith("T1"):
        LevelOnes.append(href)


# In[11]:


len(LevelOnes)


# In[12]:


# Base links
LevelOnes


# In[41]:


# Lets add Band 4 (Red) only
base="s3://landsat-pds/c1"
redlinks=[]
baselinks=[]
for link in LevelOnes:
    #print(link.split('/')[4])
    redband = base+link+"/"+link.split('/')[4]+'_B{}.TIF'.format(4)
    baselink = base+link+"/"+link.split('/')[4]
    baselinks.append(baselink)
    redlinks.append(redband)


# In[44]:


#baselinks


# In[45]:


redlinks[0]


# In[46]:


baselinks[0]


# In[48]:


import rasterio
print('Landsat on AWS:')
with rasterio.open(redlinks[0]) as src:
    print(src.profile)


# Writing a DASK task which calculates NDVI of each pixel in the large scene

# Calculate and display NDVI : Uses the MTL to calculate Landsat scene's image as reflectance values, and then we are ready to compute NDVI.
# 
# 𝑁𝐷𝑉𝐼=𝑁𝐼𝑅−𝑅𝑒𝑑𝑁𝐼𝑅+𝑅𝑒𝑑, highlights areas of healthy vegetation with high NDVI values, which appear as green in the image below.

# The Landsat Level 1 images are delivered in a quantized format. 
# This has to be converted to top-of-atmosphere reflectance using the provided metadata.

# # Conversion to TOA Reflectance
# Reflective band DN’s can be converted to TOA reflectance using the rescaling coefficients in the MTL file:  
# 
# ρλ′=Mρ * Qcal+Aρ
# 
# where:
# 
# ρλ'   = TOA planetary reflectance, without correction for solar angle.  Note that ρλ' does not contain a correction for the sun angle.
# 
# Mρ=Band-specific multiplicative rescaling factor from the metadata (REFLECTANCE_MULT_BAND_x, where x is the band number)
# 
# Aρ  =Band-specific additive rescaling factor from the metadata (REFLECTANCE_ADD_BAND_x, where x is the band number) 
# 
# Qcal =  Quantized and calibrated standard product pixel values (DN)
# 
# TOA reflectance with a correction for the sun angle is then:
# 
# ρλ=ρλ′cos(θSZ)=ρλ′sin(θSE)
# 
# where:
# 
# ρλ=  TOA planetary reflectance
# θSE =  Local sun elevation angle. 
# The scene center sun elevation angle in degrees is provided in the metadata (SUN_ELEVATION).
# θSZ =Local solar zenith angle;  θSZ = 90° - θSE
# 
# For more accurate reflectance calculations, per-pixel solar angles could be used instead of the scene center solar angle. 
# While per-pixel solar zenith angles are not provided with the Landsat Level-1 products, tools are provided which allow users to create angle bands.

# ## Be warned that TOA reflectance with a correction for the sun angle is not done

# In[83]:


import dask
import xarray as xa
import os
import json
import rasterio
import requests

@dask.delayed
def get_ndvi(item):
    # Get MTL 
    #https://landsat-pds.s3.amazonaws.com
    mtl = item +'_MTL.json'
    # A hack to get the path to download the MTL file
    mtl = mtl.replace("s3://landsat-pds", "https://landsat-pds.s3.amazonaws.com")
    if not os.path.exists(mtl):
        print("Downloading", mtl)
        response = requests.get(mtl)
        out_filename = mtl.split('.')[3].split('/')[6]+'.json'
        with open(out_filename, 'wb') as f:
            f.write(response.content)
    # The Landsat Level 1 images are delivered in a quantized format. 
    # This has to be converted to top-of-atmosphere reflectance using the provided metadata.  
    
    # One MTL file for a scene - bundled with a list of Band specific TIFF files
    with open(out_filename) as f:
        metadata = json.load(f)
        # M_p for Red
        band_number = 4
        M_p_red = metadata['L1_METADATA_FILE']                   ['RADIOMETRIC_RESCALING']                   ['REFLECTANCE_MULT_BAND_{}'.format(band_number)]
        band_number = 5
        M_p_nir = metadata['L1_METADATA_FILE']                   ['RADIOMETRIC_RESCALING']                   ['REFLECTANCE_MULT_BAND_{}'.format(band_number)]
        band_number = 4
        A_p_red = metadata['L1_METADATA_FILE']                   ['RADIOMETRIC_RESCALING']                   ['REFLECTANCE_ADD_BAND_{}'.format(band_number)]
        band_number = 5
        A_p_nir = metadata['L1_METADATA_FILE']                   ['RADIOMETRIC_RESCALING']                   ['REFLECTANCE_ADD_BAND_{}'.format(band_number)]
    
    
    # +'_B{}.TIF'.format(4)
    redband = item +'_B{}.TIF'.format(4)
    nirband = item +'_B{}.TIF'.format(5)
    
    # read red and nir
    red = xa.open_rasterio(redband, chunks={'band': 1, 'x': 1024, 'y': 1024})
    nir = xa.open_rasterio(nirband, chunks={'band': 1, 'x': 1024, 'y': 1024})
    
    red_toa = M_p_red * red + A_p_red
    nir_toa = M_p_nir * nir + A_p_nir
    
    ndvi = (nir_toa - red_toa) / (nir_toa + red_toa)

    return ndvi


# We got 126 images

# # Start a Dask client to monitor execution with the dashboard.

# In[56]:


from dask_gateway import GatewayCluster
from distributed import Client

cluster = GatewayCluster()
cluster.adapt(minimum=2, maximum=20) #Keep a minimum of 2 workers, but allow for scaling up to 20 if there is RAM and CPU pressure
client = Client(cluster) #Make sure dask knows to use this cluster for computations
cluster


#  Each scene's task execution is delayed and the operations are parallelized automatically.

# In[74]:


lazy_datasets = []
for item in baselinks:
    ds = get_ndvi(item)
    lazy_datasets.append(ds)

datasets = dask.compute(*lazy_datasets)


# In[21]:


# TODO - Get the translation using MTL to do TOA


# In[82]:


# Shut down computing resources 
client.close()
cluster.close()


# In[77]:


print(datasets[0].variable.data)


# In[89]:


len(datasets)


# # Lets inspect the NDVI of  a sample scene 

# In[91]:


### One of the latest captures (20200827)


# In[88]:


import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
plt.figure()
im = datasets[0].squeeze().compute().plot.imshow(cmap='BrBG', vmin=-0.5, vmax=1)
plt.axis('equal')
plt.show()


# In[ ]:


# one of the earliest captures (20130520)


# In[92]:


plt.figure()
im = datasets[126].squeeze().compute().plot.imshow(cmap='BrBG', vmin=-0.5, vmax=1)
plt.axis('equal')
plt.show()


# In[ ]:




