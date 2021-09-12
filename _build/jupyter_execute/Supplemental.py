#!/usr/bin/env python
# coding: utf-8

# # Supplemental notebook 

# In[1]:


# !pip3 install beautifulsoup4


# In[2]:


import requests
from bs4 import BeautifulSoup
import re
URL = 'https://landsatonaws.com/L8/227/065/'
page = requests.get(URL)

soup = BeautifulSoup(page.content, 'html.parser')


# In[3]:


for link in soup.findAll('a', attrs={'href': re.compile("^/L8/227/065")}):
    print(link.get('href'))


# In[ ]:




