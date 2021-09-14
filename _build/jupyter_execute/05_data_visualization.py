#!/usr/bin/env python
# coding: utf-8

# ## Data Visualization
# 
# ### Questions
# - How do I visualize results using open source software?
# 
# ### Objectives
# - Learn about Holoviews for data visualization

# In[1]:


import numpy as np
import xarray as xr
import holoviews as hv
from holoviews import opts
import geoviews as gv
import datashader as ds
import cartopy.crs as ccrs
import xarray as xa
from holoviews.operation.datashader import regrid, shade
from bokeh.tile_providers import OSM

get_ipython().run_line_magic('matplotlib', 'inline')
hv.extension('bokeh', width=80)


# Check for single band

# In[2]:


url = 'http://landsat-pds.s3.amazonaws.com/c1/L8/227/065/LC08_L1TP_227065_20200608_20200626_01_T1/'
redband = url+'LC08_L1TP_227065_20200608_20200626_01_T1_B{}.TIF'.format(4)

redband


# Read in all the bands

# In[3]:


file_path = 'http://landsat-pds.s3.amazonaws.com/c1/L8/227/065/LC08_L1TP_227065_20200608_20200626_01_T1/LC08_L1TP_227065_20200608_20200626_01_T1_B%s.TIF'
bands = list(range(1, 12))
bands = [xr.open_rasterio(file_path%band).load()[0] for band in bands]


# In[4]:


bands


# In[5]:


opts.defaults(opts.RGB(width=600, height=600))

nodata= 1

def one_band(b):
    xs, ys = b['x'], b['y']
    b = ds.utils.orient_array(b)
    a = (np.where(np.logical_or(np.isnan(b),b<=nodata),0,255)).astype(np.uint8)    
    col, rows = b.shape
    return hv.RGB((xs, ys[::-1], b, b, b, a), vdims=list('RGBA'))

# not working through GeoViews
#tiles = gv.WMTS(OSM)
tiles = hv.element.tiles.EsriUSATopo().opts(width=600, height=550)


# In[6]:


tiles*shade(regrid(one_band(bands[1])), cmap=['black', 'white']).redim(x='Longitude', y='Latitude')


# Add background tiles

# In[ ]:





# In[7]:


from datashader.utils import ngjit
nodata= 1

@ngjit
def normalize_data(agg):
    out = np.zeros_like(agg)
    min_val = 0
    max_val = 2**16 - 1
    range_val = max_val - min_val
    col, rows = agg.shape
    c = 40
    th = .125
    for x in range(col):
        for y in range(rows):
            val = agg[x, y]
            norm = (val - min_val) / range_val
            norm = 1 / (1 + np.exp(c * (th - norm))) # bonus
            out[x, y] = norm * 255.0
    return out

def combine_bands(r, g, b):
    xs, ys = r['x'], r['y']
    r, g, b = [ds.utils.orient_array(img) for img in (r, g, b)]
    a = (np.where(np.logical_or(np.isnan(r),r<=nodata),0,255)).astype(np.uint8)    
    r = (normalize_data(r)).astype(np.uint8)
    g = (normalize_data(g)).astype(np.uint8)
    b = (normalize_data(b)).astype(np.uint8)
    return hv.RGB((xs, ys[::-1], r, g, b, a), vdims=list('RGBA'))


# In[8]:


# Lets visualize area of in true color


# In[9]:


true_color = combine_bands(bands[3], bands[2], bands[1]).relabel("True Color (R=Red, G=Green, B=Blue)")
regrid(true_color)


# In[ ]:





# In[10]:


import pandas as pd
band_info = pd.DataFrame([
        (1,  "Aerosol", " 0.43 - 0.45",    0.440,  "30",         "Coastal aerosol"),
        (2,  "Blue",    " 0.45 - 0.51",    0.480,  "30",         "Blue"),
        (3,  "Green",   " 0.53 - 0.59",    0.560,  "30",         "Green"),
        (4,  "Red",     " 0.64 - 0.67",    0.655,  "30",         "Red"),
        (5,  "NIR",     " 0.85 - 0.88",    0.865,  "30",         "Near Infrared (NIR)"),
        (6,  "SWIR1",   " 1.57 - 1.65",    1.610,  "30",         "Shortwave Infrared (SWIR) 1"),
        (7,  "SWIR2",   " 2.11 - 2.29",    2.200,  "30",         "Shortwave Infrared (SWIR) 2"),
        (8,  "Panc",    " 0.50 - 0.68",    0.590,  "15",         "Panchromatic"),
        (9,  "Cirrus",  " 1.36 - 1.38",    1.370,  "30",         "Cirrus"),
        (10, "TIRS1",   "10.60 - 11.19",   10.895, "100 * (30)", "Thermal Infrared (TIRS) 1"),
        (11, "TIRS2",   "11.50 - 12.51",   12.005, "100 * (30)", "Thermal Infrared (TIRS) 2")],
    columns=['Band', 'Name', 'Wavelength Range (µm)', 'Nominal Wavelength (µm)', 'Resolution (m)', 'Description']).set_index(["Band"])
band_info


# In[11]:


combos = pd.DataFrame([
    (4,3,2,"True color",""),
    (7,6,4,"Urban","False color"),
    (5,4,3,"Vegetation","Color Infrared"),
    (6,5,2,"Agriculture",""),
    (7,6,5,"Penetration","Atmospheric Penetration"),
    (5,6,2,"Healthy Vegetation",""),
    (5,6,4,"Land vs. Water",""),
    (7,5,3,"Atmosphere Removal","Natural With Atmospheric Removal"),
    (7,5,4,"Shortwave Infrared",""),
    (6,5,4,"Vegetation Analysis","")],
    columns=['R', 'G', 'B', 'Name', 'Description']).set_index(["Name"])
combos

def combo(name):
    c=combos.loc[name]
    return regrid(combine_bands(bands[c.R-1],bands[c.G-1],bands[c.B-1])).relabel(name)

layout = combo("Urban") + combo("Vegetation") + combo("Agriculture") + combo("Land vs. Water")

layout.opts(
    opts.RGB(width=350, height=350, xaxis=None, yaxis=None, framewise=True)).cols(2)


# Explore the full hyperspectral information at any location (here Amazon forest) in the true-color image. Click the point on the left-  updates the spectral curve whenever you hover over an area of the image:

# In[13]:


band_map = hv.HoloMap({i: hv.Image(band) for i, band in enumerate(bands)})

def spectrum(x, y):
    try: 
        spectrum_vals = band_map.sample(x=x, y=y)['z'][:-1]
        point = gv.Points([(x, y)], crs=ccrs.GOOGLE_MERCATOR)
        point = gv.operation.project_points(point, projection=ccrs.PlateCarree())
        label = label = 'Lon: %.3f, Lat: %.3f' % tuple(point.array()[0])
    except:
        spectrum_vals = np.zeros(11)
        label = 'Lon: -, Lat: -'
    
    return hv.Curve((band_info['Nominal Wavelength (µm)'].values, spectrum_vals), label=label,
                    kdims=['Wavelength (µm)'], vdims=['Luminance']).sort()

# x and y give the location in Web Mercator coordinates
spectrum(x=700000, y=-800000).opts(width=800, height=300, logx=True)


# In[14]:


tap = hv.streams.PointerXY(source=true_color)
spectrum_curve = hv.DynamicMap(spectrum, streams=[tap]).redim.range(Luminance=(0, 30000))

layout = regrid(true_color) + spectrum_curve
layout.opts(
    opts.Curve(width=450, height=450, logx=True),
    opts.RGB(width=450, height=450))


# In[15]:


# Can change the background tiles now
# see here https://holoviews.org/reference/elements/bokeh/Tiles.html


# In[16]:


tiles = hv.element.tiles.StamenWatercolor().opts(width=600, height=550)


# In[17]:


layout = tiles * regrid(true_color) + spectrum_curve
layout.opts(
    opts.Curve(width=450, height=450, logx=True),
    opts.RGB(width=450, height=450))


# Point it to particular location

# Check ESRI imagery

# In[18]:


hv.element.tiles.EsriImagery().opts(width=600, height=550)


# In[26]:


#lets visualize the ndvi
ndvi = (bands[5]-bands[4]) / (bands[5] + bands[4])


# In[28]:


def ndvi(r, nir):  
    r = (normalize_data(r))
    nir = (normalize_data(nir))
    return((r-nir)/(r+nir))


# In[ ]:





# In[ ]:





# In[ ]:




