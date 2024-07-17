from datetime import datetime
import time
from tools_sku_price import SkuTools
from sp_api.base import Marketplaces
from ad_api.base.marketplaces import Currencies
from ai.backend.util.db.auto_process.selling_partner.db_tool.tools_db import DbTools

def update_sku_price(market, sku, new_price):
    apitool = SkuTools()
    list_info = apitool.list_item_api(market,sku)
    print(list_info)
    asin = list_info['summaries'][0]['asin']
    productType = list_info['summaries'][0]['productType']
    for offer in list_info['offers']:
        if offer['offerType'] == 'B2C':
            old_price = offer['price']['amount']
            break
    sku_info = {
  "productType":productType,
  "patches":[
      {
          "op": "replace",
          "path": "/attributes/purchasable_offer",
          "value": [
              {
                  "marketplace_id": Marketplaces[market].marketplace_id,
                  "currency": Currencies[market].value,
                  "our_price": [
                      {
                          "schedule": [
                              {
                                  "value_with_tax": new_price
                              }
                          ]
                      }
                  ]
              }
          ]
      }
    # {
    #   "op":"replace",
    #   "path":"/offers",
    #   "value":[
    #     {
    #       "offerType": "B2C",
    #       "marketplace_id": Marketplaces[market].marketplace_id,
    #       'price': {
    #           'currency': Currencies[market].value,
    #           'currencyCode': Currencies[market].value,
    #           'amount': new_price
    #       }
    #     }
    #   ]
    # }
  ]
}
    # 调用api

    apires = apitool.update_item_api(market, sku, sku_info)

    # 更新log
    #     def update_sp_campaign(self,market,campaign_name,campaign_id,budget_old,budget_new,standards_acos,acos,beizhu,status,update_time):
    update_time = datetime.now()
    newdbtool = DbTools()
    if apires[0] == "success":
        print("api update success")
        id = newdbtool.update_sku_price(market, sku, asin, old_price, new_price, "success", update_time,None,None)

        time.sleep(60 * 60)
        list_info1 = apitool.list_item_api(market, sku)
        for offer in list_info1['offers']:
            if offer['offerType'] == 'B2C':
                amount = offer['price']['amount']
                break
        newdbtool.check_sku_price(amount, datetime.now(), id)
    else:
        print("api update failed")
        newdbtool.update_sku_price(market, sku, asin, None, new_price, "failed",update_time,None,None)




update_sku_price('FR', 'LPK17SS0001MT020XSR4',29.99)
