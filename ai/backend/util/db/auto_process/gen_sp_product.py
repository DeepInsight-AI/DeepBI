from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools
from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime
from ai.backend.util.db.auto_process.tools_sp_product import ProductTools


class Gen_product(ProductTools):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

# 创建/新增品
    def create_productsku(self,campaignId,adGroupId,sku,asin,state,user='test'):
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
        adId = self.create_product_api(product_info)
        print(adId)

        # 如果执行成功或者失败 记录到log表记录
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        if adId[0]=="success":
            dbNewTools.create_sp_product(self.market,campaignId,asin,sku,adGroupId,adId[1],"success",datetime.now(),"SP",user)
        else:
            dbNewTools.create_sp_product(self.market,campaignId,asin,sku,adGroupId,adId[1],"failed",datetime.now(),"SP",user)
        return adId[1]
    # 新增测试
    # create_productsku('FR','284793893968513','B075SWSWHR','PAUSED','LPM17SS0035MT0300LR4','397527887041271')


    # 修改品的信息  - 暂时只能修改品的状态
    def update_product(self,adId,state, user='test'):
        product_info = {
            "productAds": [
                {
                    "adId": adId,
                    "state": state
                }
            ]
        }
        # 执行修改品
        adIdres = self.update_product_api(product_info)
        print(adIdres)
        # 如果执行成功或者失败 记录到log表记录
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        if adIdres[0] == "success":
            dbNewTools.update_sp_product(self.market, adId, state, "success", datetime.now(), user)
        else:
            dbNewTools.update_sp_product(self.market, adId, state, "failed", datetime.now(), user)

    #修改品测试
    # update_product('US','366708088753798','ENABLED')
if __name__ == '__main__':
    info = ['354879095966863', '433117102035287', '362317923703042', '467016438165555', '537730943121270', '293731204761757', '300725268282095', '351383885391593', '301629547736857', '524841711595199', '290290289146536', '386493185013832', '351258704110344', '413439313050358', '363500909645504', '445998018893150', '505354097079035', '402710004065917', '514097677446213', '498677816414906', '307874334358337', '426288239836427', '327533929093589', '361264841509534', '421146318413438', '418021240506408', '322520597232626', '482297345336146', '361981705896098', '348942915035801', '330956717350973', '464002677462430', '508346559944049', '456024984716010', '519337935356264', '400352875729739']
    for i in info:
        Gen_product('amazon_ouruite','RIDALUX','US').update_product(i,'PAUSED')
