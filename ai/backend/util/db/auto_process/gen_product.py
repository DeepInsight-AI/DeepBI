from tools_sp_adGroup import AdGroupTools
from tools_db_sp import DbSpTools
from tools_db_new_sp import DbNewSpTools
from datetime import datetime

from tools_sp_product import ProductTools
db_info = {'host': '****', 'user': '****', 'passwd': '****', 'port': 3306,
               'db': '****',
               'charset': 'utf8mb4', 'use_unicode': True, }

# 创建/新增品
def create_productsku(market,campaignId,adGroupId,sku,asin,state):
    product_info = {
  "productAds": [
    {
      "campaignId": str(campaignId),
      "state": state,
      "sku": sku,
      "asin": asin,
      "adGroupId": str(adGroupId)
    }
  ]
}
    # 执行新增品 返回adId
    apitoolProduct=ProductTools()
    adId = apitoolProduct.create_product_api(product_info,market)
    print(adId)

    # 如果执行成功或者失败 记录到log表记录
    dbNewTools = DbNewSpTools()
    if adId[0]=="success":
        dbNewTools.create_sp_product(market,campaignId,asin,sku,adGroupId,adId[1],"success",datetime.now())
    else:
        dbNewTools.create_sp_product(market,campaignId,asin,sku,adGroupId,adId[1],"failed",datetime.now())
    return adId[1]
# 新增测试
# create_productsku('FR','284793893968513','B075SWSWHR','PAUSED','LPM17SS0035MT0300LR4','397527887041271')


# 修改品的信息  - 暂时只能修改品的状态
def update_product(market,adId,state):
    product_info = {
        "productAds": [
            {
                "adId": adId,
                "state": state
            }
        ]
    }
    # 执行修改品
    apitoolProduct = ProductTools()
    adIdres = apitoolProduct.update_product_api(product_info)
    print(adIdres)
    # 如果执行成功或者失败 记录到log表记录
    dbNewTools = DbNewSpTools()
    if adIdres[0] == "success":
        dbNewTools.update_sp_product(market, adId, state, "success", datetime.now())
    else:
        dbNewTools.update_sp_product(market, adId, state, "failed", datetime.now())

#修改品测试
# update_product('US','366708088753798','ENABLED')

