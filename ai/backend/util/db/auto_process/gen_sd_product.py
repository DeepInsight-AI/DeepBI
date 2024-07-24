# from tools_sp_adGroup import AdGroupTools
# from tools_db_sp import DbSpTools
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime
from ai.backend.util.db.auto_process.tools_sd_product import ProductTools

class Gen_product:
    def __init__(self,brand):
        self.brand = brand
# 创建/新增品
    def create_productsku(self,market,campaignId,adGroupId,sku,state):
        product_info = [
      {
        "state": state,
        "adGroupId": adGroupId,
        "campaignId": campaignId,
        "sku": sku
      }
    ]
        # 执行新增品 返回adId
        apitoolProduct=ProductTools(self.brand)
        adId = apitoolProduct.create_product_api(product_info,market)
        print(adId)

        # 如果执行成功或者失败 记录到log表记录
        dbNewTools = DbNewSpTools(self.brand)
        if adId[0]=="success":
            dbNewTools.create_sp_product(market,campaignId,None,sku,adGroupId,adId[1],"success",datetime.now(),"SD")
        else:
            dbNewTools.create_sp_product(market,campaignId,None,sku,adGroupId,adId[1],"failed",datetime.now(),"SD")
        return adId[1]
    # 新增测试
    # create_productsku('FR','284793893968513','B075SWSWHR','PAUSED','LPM17SS0035MT0300LR4','397527887041271')


    # 修改品的信息  - 暂时只能修改品的状态
    def update_product(self,market,adId,state):
        product_info = [
  {
    "state": state,
    "adId": adId
  }
]
        # 执行修改品
        apitoolProduct = ProductTools(self.brand)
        adIdres = apitoolProduct.update_product_api(product_info,market)
        print(adIdres)
        # 如果执行成功或者失败 记录到log表记录
        dbNewTools = DbNewSpTools(self.brand)
        if adIdres[0] == "success":
            dbNewTools.update_sp_product(market, adId, state, "success", datetime.now())
        else:
            dbNewTools.update_sp_product(market, adId, state, "failed", datetime.now())

    #修改品测试
    # update_product('US','366708088753798','ENABLED')

    def create_creatives(self,market,adGroupId):
        if self.brand == 'LAPASA':
            if market == 'US':
                assetId = 'amzn1.assetlibrary.asset1.a0f06706a4ac4a5f033bf740de9c0cdc'
                width = 2363
                height = 2363
            elif market == 'FR':
                assetId = 'amzn1.assetlibrary.asset1.885ba38c21ef528785710ac7e9594479'
                width = 945
                height = 945
            elif market == 'NL':
                assetId = 'amzn1.assetlibrary.asset1.3f5e2beb57a36599a3a546941123f267'
                width = 945
                height = 945
            elif market == 'ES':
                assetId = 'AWqK9cultC_knw3G930F'
                width = 1667
                height = 1667
            elif market == 'IT':
                assetId = 'amzn1.assetlibrary.asset1.bb1b8ad23c7a461182b201524cb33efb'
                width = 945
                height = 945
            elif market == 'SE':
                assetId = 'amzn1.assetlibrary.asset1.d0994a669899da4306649ae715fc83eb'
                width = 945
                height = 945
            elif market == 'DE':
                assetId = 'amzn1.assetlibrary.asset1.7e14ad0f9e26db21ff3ae1403ee64264'
                width = 2363
                height = 2363
            elif market == 'UK':
                assetId = 'AWTp8HtljX_6uu4JfZcR'
                width = 1667
                height = 1667
            elif market == 'JP':
                assetId = 'amzn1.assetlibrary.asset1.72c3584093a109f03036ac406aa7f47c'
                width = 945
                height = 945
        elif self.brand == 'DELOMO':
            if market == 'US':
                assetId = None
                length = None
            elif market == 'FR':
                assetId = 'amzn1.assetlibrary.asset1.77466c21fcab5437fcf86c4407f89af5'
                width = 600
                height = 300
            elif market == 'ES':
                assetId = 'amzn1.assetlibrary.asset1.9c64e8eefc270982f5d0702784aa1e85'
                width = 910
                height = 681
            elif market == 'DE':
                assetId = 'amzn1.assetlibrary.asset1.e3d0f7016f51d26049b892a923a924b7'
                width = 600
                height = 300
        elif self.brand == 'OutdoorMaster':
            if market == 'US':
                assetId = None
                length = None
            elif market == 'FR':
                assetId = 'amzn1.assetlibrary.asset1.a3b77183914ab79d7873a7b0c52d9536'
                width = 966
                height = 413
            elif market == 'ES':
                assetId = 'amzn1.assetlibrary.asset1.24875f9f5e48e43c01d4827f1e7fb762'
                width = 966
                height = 413
            elif market == 'IT':
                assetId = 'amzn1.assetlibrary.asset1.3da650408bdf2685503751aaa9df8969'
                width = 966
                height = 413
            elif market == 'SE':
                assetId = 'amzn1.assetlibrary.asset1.39ea0458e01e8879cbf5d36700807c4c'
                width = 966
                height = 413
        creatives_info = [
      {
        "adGroupId": adGroupId,
        "properties": {
            'brandLogo': {
                'assetId': assetId,
                'assetVersion': 'version_v1',
                'croppingCoordinates': {
                    'top': 0,
                    'left': 0,
                    'width': width,
                    'height': height
                }
            }
        }
      }
    ]
        # 执行新增品 返回adId
        apitoolProduct=ProductTools(self.brand)
        creativeId = apitoolProduct.create_creatives_api(market,creatives_info)
        print(creativeId)

        # 如果执行成功或者失败 记录到log表记录
        # dbNewTools = DbNewSpTools()
        # if adId[0]=="success":
        #     dbNewTools.create_sp_product(market,campaignId,asin,sku,adGroupId,adId[1],"success",datetime.now())
        # else:
        #     dbNewTools.create_sp_product(market,campaignId,asin,sku,adGroupId,adId[1],"failed",datetime.now())
        return creativeId


#create_creatives('NL',{'brandLogo': {'assetId': 'amzn1.assetlibrary.asset1.885ba38c21ef528785710ac7e9594479', 'assetVersion': 'version_v1', 'croppingCoordinates': {'top': 0, 'left': 0, 'width': 945, 'height': 945}}},314554856887145)
