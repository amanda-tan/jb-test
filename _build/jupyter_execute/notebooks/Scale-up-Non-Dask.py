#!/usr/bin/env python
# coding: utf-8

# # Goal of this notebook is to scale through a cluster to calculate NDVI across 128+ Landsat Scenes (using DASK)

# ## Uses rio-xarray pkg

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


import xarray as xa
red = xa.open_rasterio('LC08_L1TP_227065_20200608_20200626_01_T1_B4.TIF')


# In[26]:


red.crs


# In[27]:


red.shape[2],red.shape[1]


# In[ ]:





# In[ ]:





# In[29]:


red


# In[30]:


import rioxarray
import xarray

#xds = xarray.open_rasterio("myfile.tif")
#wgs84_xds = xds.rio.reproject("EPSG:4326")
red.rio.to_raster("myfile_wgs84.tif")


# In[31]:


import rasterio
from rasterio.plot import show
src = rasterio.open("myfile_wgs84.tif")

show(src)


# In[32]:


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

# In[33]:


from osgeo import gdal

input_raster=gdal.Open('LC08_L1TP_227065_20200608_20200626_01_T1_B4.TIF')
output_raster = "Reproj" + 'LC08_L1TP_227065_20200608_20200626_01_T1_B4.TIF'
gdal.Warp(output_raster,input_raster,dstSRS='EPSG:4326')


# In[34]:


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

# In[3]:


#get list of images
import requests
from bs4 import BeautifulSoup
import re
URL = 'https://landsatonaws.com/L8/227/065/'
page = requests.get(URL)

soup = BeautifulSoup(page.content, 'html.parser')


# In[4]:


# Use beautiful soup to extract all the links, and Tier 1 only
LevelOnes=[]
for link in soup.findAll('a', attrs={'href': re.compile("^/L8/227/065")}):
    #print(link.get('href'))
    href = link.get('href')
    if href.endswith("T1"):
        LevelOnes.append(href)


# In[5]:


len(LevelOnes)


# In[6]:


# Base links
LevelOnes


# In[7]:


LevelOnes=['/L8/227/065/LC08_L1TP_227065_20201201_20201217_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20201115_20201210_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200928_20201007_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200912_20200920_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200827_20200905_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200827_20200905_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200811_20200822_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200726_20200807_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200710_20200721_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200624_20200707_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200608_20200626_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200608_20200626_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200523_20200607_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200507_20200509_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200405_20200410_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20200217_20200225_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20191215_20201023_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20191215_20191226_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20191129_20191216_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20191028_20191114_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20191012_20191018_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190926_20191017_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190910_20190917_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190825_20190903_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190724_20190801_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190708_20190719_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190622_20190704_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190606_20190619_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190505_20190520_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190505_20190520_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190419_20190423_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20190318_20190325_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20181126_20181210_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20181126_20181210_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20181110_20181127_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20181025_20181114_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20181009_20181029_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180923_20180929_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180923_20180929_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180907_20180912_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180907_20180912_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180822_20180829_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180806_20180815_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180721_20180731_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180705_20180717_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180705_20180717_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180705_20180717_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180619_20180703_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180619_20180703_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180518_20180604_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180502_20180516_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180227_20180308_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20180110_20180119_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20171123_20171206_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20171022_20171107_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20171006_20171023_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170904_20170916_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170904_20170916_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170904_20170916_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170904_20170916_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170819_20170826_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170803_20170812_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170718_20170727_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170718_20170727_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170718_20170727_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170718_20170727_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170702_20170715_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170616_20170629_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170616_20180528_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170616_20170629_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170531_20170615_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170515_20170525_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170413_20170501_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170312_20170317_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20170107_20170311_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20161206_20180528_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20161104_20170318_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20161003_20170320_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160917_20170321_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160901_20170321_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160816_20170322_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160731_20170322_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160715_20170323_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160629_20170323_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160613_20170324_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160528_20170324_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160512_20170324_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160426_20170326_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160410_20170326_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160222_20170329_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20160206_20170330_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20151204_20170401_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20151118_20170402_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20151102_20170402_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20151017_20170403_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20151001_20170403_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150915_20170404_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150830_20170405_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150814_20170406_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150729_20170406_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150713_20170407_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150627_20170407_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150611_20170408_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150526_20170408_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150510_20170409_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150424_20170409_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150408_20170410_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150203_20170413_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20150102_20170415_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20141217_20170416_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20141115_20180528_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20141014_20170418_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140928_20170419_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140912_20170419_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140827_20170420_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140811_20170420_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140726_20170421_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140710_20170421_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140624_20170421_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140608_20170422_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140523_20170422_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140507_20170422_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140421_20170423_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20140320_20170425_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20131011_20170429_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20130909_20170502_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20130824_20170502_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20130808_20170503_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20130723_20170503_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20130707_20170503_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20130621_20170503_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20130605_20170504_01_T1',
 '/L8/227/065/LC08_L1TP_227065_20130520_20170504_01_T1']


# In[8]:


### Populate the collection of scenes


# In[9]:


# Lets add Band 4 (Red) only
base="http://landsat-pds.s3.amazonaws.com/c1"
redlinks=[]
baselinks=[]
for link in LevelOnes:
    #print(link.split('/')[4])
    redband = base+link+"/"+link.split('/')[4]+'_B{}.TIF'.format(4)
    #print(redband)
    baselink = base+link+"/"+link.split('/')[4]
    baselinks.append(baselink)
    redlinks.append(redband)


# In[10]:


len(LevelOnes)


# In[ ]:





# In[11]:


redband


# In[ ]:





# In[12]:


baselinks[0].split('/')[7]


# In[13]:


import rasterio
print('Landsat on AWS:')
with rasterio.open(redlinks[0]) as src:
    print(src.profile)


# Writing a non-DASK task which calculates NDVI of each pixel in the large scene

# Calculate and display NDVI : Uses the MTL to calculate Landsat scene's image as reflectance values, and then we are ready to compute NDVI.
# 
# 𝑁𝐷𝑉𝐼=𝑁𝐼𝑅−𝑅𝑒𝑑𝑁𝐼𝑅+𝑅𝑒𝑑, highlights areas of healthy vegetation with high NDVI values, which appear as green in the image below.

# The Landsat Level 1 images are delivered in a quantized format. 
# This has to be converted to top-of-atmosphere reflectance using the provided metadata.

# ## Be warned that TOA reflectance with a correction for the sun angle is not done

# # Conversion to TOA Reflectance
# ### Ref - https://www.usgs.gov/core-science-systems/nli/landsat/using-usgs-landsat-level-1-data-product
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

# In[24]:


import dask
import xarray as xa
import os
import json
import rasterio
import requests
import numpy as np
import rioxarray
import xarray
import matplotlib.pyplot as plt

#xds = xarray.open_rasterio("myfile.tif")
#wgs84_xds = xds.rio.reproject("EPSG:4326")

def calculate_ndvi_rioxarray(item):
    # reclassed count
    reclassed=[]
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
    red = rioxarray.open_rasterio(redband, chunks={'band': 1, 'x': 1024, 'y': 1024})
    nir = rioxarray.open_rasterio(nirband, chunks={'band': 1, 'x': 1024, 'y': 1024})
    
    red_toa = M_p_red * red + A_p_red
    nir_toa = M_p_nir * nir + A_p_nir
    
    ndvi = (nir_toa - red_toa) / (nir_toa + red_toa)
    
    # split
    name = item.split('/')[7]
    
    #save
    ndvi.rio.to_raster(
        "./data/vegetation/ndvi/"+name+".tiff",
        tiled=True,
    )
    
    # Define class names
    ndvi_cat_names = [
    "No Vegetation",
    "Bare Area",
    "Low Vegetation",
    "Moderate Vegetation",
    "High Vegetation",
    ]

    with rasterio.open("./data/vegetation/ndvi/"+name+".tiff") as src:    
        array = src.read()
        profile = src.profile
        bins = np.array([-np.inf, 0, 0.1, 0.25, 0.4, np.inf]) 
        inds = np.digitize(array, bins)
        ind64 = np.float64(inds)
        unique_elements, counts_elements = np.unique(ind64, return_counts=True)
        print("Frequency of unique values of the said array:")
        print(np.asarray((unique_elements, counts_elements)))
        reclassed = np.asarray((unique_elements, counts_elements))
    

    with rasterio.open('./data/vegetation/ndvi/reclass_ndvi_'+name+".tiff", 'w', **profile) as dst:
        dst.write(ind64)
    
    os.remove("./data/vegetation/ndvi/"+name+".tiff")
    os.remove('./data/vegetation/ndvi/reclass_ndvi_'+name+".tiff")

    return {"tile":name,"var":reclassed}


# In[20]:


baselinks[0]


# In[25]:


recl= calculate_ndvi_rioxarray(baselinks[0])


# In[26]:


recl


# In[27]:


state_datasets = []
for item in baselinks:
    ds = calculate_ndvi_rioxarray(item)
    state_datasets.append(ds)


# In[28]:


state_datasets


# In[ ]:





# In[14]:


get_ipython().system('pip3 install earthpy')


# In[30]:


# Sample run


# In[ ]:


import matplotlib.pyplot as plt
import earthpy.plot as ep
import earthpy.spatial as es
from rasterio.plot import show
from matplotlib.colors import ListedColormap
item = baselinks[2]
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
    
    # split
    name = item.split('/')[7]
    #
    print(nirband)
    print(redband)
    # read red and nir
    red = rioxarray.open_rasterio(redband)
    nir = rioxarray.open_rasterio(nirband)
    
    red_toa = M_p_red * red + A_p_red
    nir_toa = M_p_nir * nir + A_p_nir
    f, ax = plt.subplots(figsize=(10, 6))
    red_toa.plot.hist(ax=ax,
       color="purple")
    ax.set(title="Distribution of red",
       xlabel='reflectance',
       ylabel='Frequency')
    #plt.show()
    
    f, ax = plt.subplots(figsize=(10, 6))
    nir_toa.plot.hist(ax=ax,
       color="purple")
    ax.set(title="Distribution of nir",
       xlabel='reflectance',
       ylabel='Frequency')
    #plt.show()
    
    ndvi = (nir_toa - red_toa) / (nir_toa + red_toa)
    lndsat_ndvi = es.normalized_diff(nir_toa, red_toa)
    
    #save
    ndvi.rio.to_raster(
        "./data/vegetation/ndvi/"+name+".tiff",
        tiled=True,
    )
    #show(ndvi, cmap='viridis')
    ep.plot_bands(lndsat_ndvi,
              cmap='PiYG',
              scale=False,
              vmin=-1, vmax=1,
              title="NAIP Derived NDVI\n 19 September 2015 - Cold Springs Fire, Colorado")
    plt.show()
    
    # Create classes and apply to NDVI results
    ndvi_class_bins = [-np.inf, 0, 0.1, 0.25, 0.4, np.inf]
    ndvi_landsat_class = np.digitize(lndsat_ndvi, ndvi_class_bins)

    # Apply the nodata mask to the newly classified NDVI data
    ndvi_landsat_class = np.ma.masked_where(
                np.ma.getmask(lndsat_ndvi), ndvi_landsat_class
        )
    np.unique(ndvi_landsat_class)
    
    # Get list of classes
    classes = np.unique(ndvi_landsat_class)
    classes = classes.tolist()
    # The mask returns a value of none in the classes. remove that
    classes = classes[0:5]
    
    print("classes", classes)
    
    ndvi_landsat_class.plot.hist(ax=ax,
       color="purple")
    ax.set(title="Distribution of reclass",
       xlabel='reflectance',
       ylabel='Frequency')
    
    # Define color map
    nbr_colors = ["gray", "y", "yellowgreen", "g", "darkgreen"]
    nbr_cmap = ListedColormap(nbr_colors)

    # Define class names
    ndvi_cat_names = [
    "No Vegetation",
    "Bare Area",
    "Low Vegetation",
    "Moderate Vegetation",
    "High Vegetation",
    ]

    # Get list of classes
    classes = np.unique(ndvi_landsat_class)
    classes = classes.tolist()
    # The mask returns a value of none in the classes. remove that
    classes = classes[0:5]


# In[1]:


import rasterio
from matplotlib import pyplot
src = rasterio.open("./data/vegetation/ndvi/LC08_L1TP_227065_20200928_20201007_01_T1.tiff")
pyplot.imshow(src.read(1),vmin=-1, vmax=1,cmap='PiYG')
pyplot.show()


# In[4]:


import rasterio
import numpy as np

with rasterio.open('./data/vegetation/ndvi/LC08_L1TP_227065_20200928_20201007_01_T1.tiff') as src:    
    array = src.read()
    profile = src.profile
    bins = np.array([-np.inf, 0, 0.1, 0.25, 0.4, np.inf]) 
    inds = np.digitize(array, bins)
    ind64 = np.float64(inds)
    
    unique_elements, counts_elements = np.unique(ind64, return_counts=True)
    print("Frequency of unique values of the said array:")
    print(np.asarray((unique_elements, counts_elements)))

    with rasterio.open('./data/vegetation/ndvi/reclass_ndvi_'+ 'LC08_L1TP_227065_20200928_20201007_01_T1.tiff', 'w', **profile) as dst:
        dst.write(ind64)


# In[7]:


import rasterio
from matplotlib import pyplot
import matplotlib.pyplot as plt
import rioxarray
src = rasterio.open("./data/vegetation/ndvi/reclass_ndvi_LC08_L1TP_227065_20200928_20201007_01_T1.tiff")
pyplot.imshow(src.read(1),cmap='PiYG')
pyplot.show()

ndvi_reclass = rioxarray.open_rasterio('./data/vegetation/ndvi/reclass_ndvi_LC08_L1TP_227065_20200928_20201007_01_T1.tiff')
f, ax = plt.subplots(figsize=(10, 6))
ndvi_reclass.plot.hist(ax=ax,
       color="purple")
ax.set(title="Distribution of red",
       xlabel='reflectance',
       ylabel='Frequency')


# In[8]:


get_ipython().system('rio info ./data/vegetation/ndvi/reclass_ndvi_LC08_L1TP_227065_20200928_20201007_01_T1.tiff')


# In[ ]:




