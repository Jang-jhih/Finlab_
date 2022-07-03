
#%%
# 修正爬蟲

from jacob.crawlar import *
# from jacob.financal_statement import *
crawl_price()

#%%
year = 2021
season = 1
[crawl_finance_statement(year,season) for season in range(1,5)]


crawl_finance_statement(year,1)
