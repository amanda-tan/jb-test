#!/usr/bin/env python
# coding: utf-8

# ## Parallel Computing with Dask
# ### Questions
# - How do I start working with larger datasets in parallel? 
# 
# ### Objectives
# - Introduce the concept of Dask, a free and open-source library for parallel computing in Python

# In[1]:


import dask
from dask.distributed import Client
client = Client(processes=False)
client


# In[57]:


import xarray as xa
def calc_stats(url):
    bf = xa.open_rasterio(url, chunks={'band': 1, 'x': 1024, 'y': 1024})
    mean_band= bf.mean()
    return mean_band


# Test it locally 

# In[58]:


url = 'http://landsat-pds.s3.amazonaws.com/c1/L8/227/065/LC08_L1TP_227065_20200608_20200626_01_T1/'
redband = url+'LC08_L1TP_227065_20200608_20200626_01_T1_B{}.TIF'.format(4)

redband


# In[59]:


mean=calc_stats(redband)


# We will use client.submit to execute the computation on a distributed worker:

# In[60]:


future = client.submit(calc_stats, redband)


# In[61]:


future


# Lets do two files

# We are now ready to get mean across many files using distributed workers. We can use map operation which is non-blocking, and one can continue to work in the Python shell/notebook while the computations are running.

# In[62]:


b4 = url+'LC08_L1TP_227065_20200608_20200626_01_T1_B{}.TIF'.format(4)
b5 = url+'LC08_L1TP_227065_20200608_20200626_01_T1_B{}.TIF'.format(5)
b6 = url+'LC08_L1TP_227065_20200608_20200626_01_T1_B{}.TIF'.format(6)
filenames=[b4,b5,b6]


# In[63]:


futures = client.map(calc_stats, filenames)


# In[64]:


len(futures)


# In[65]:


futures[:3]


# In[66]:


from distributed import progress


# In[67]:


#progress(futures)


# In[68]:


progress(futures)


# Results come in Dask array that are essentially Numpy. You can call .compute(), it will give result as a NumPy array.

# In[69]:


# You can get output of mean Reflectances in a band
means = client.gather(futures)


# In[71]:


# Mean of second Red Band, remember its scaled-up
means[2].compute()

