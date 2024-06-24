from ai.backend.util.db.auto_yzj.utils.find import find_files,find_file_by_name
from ai.backend.util.db.auto_process.update_sp_ad_auto import update_sp_ad_keyword,update_sp_ad_budget,update_sp_ad_placement,add_sp_ad_searchTerm_keyword,add_sp_ad_searchTerm_negative_keyword,update_sp_ad_sku,update_sp_ad_automatic_targeting


def auto_execute(cur_time: str, country: str, version: int = 1):
    suffix = country + '_' + cur_time + '.csv'
    #suffix = '自动_劣质广告活动_IT_2024-06-19.csv'
    mds = find_files(directory="./日常优化/", suffix=suffix)
    print(mds)
    if len(mds) > 0:
        for md in mds:
            budget = "广告活动"
            targeting_group = "广告位"
            search_term_good = "优质搜索词"
            search_term_bad = "劣质搜索词"
            sku = "SKU"
            keyword = "关键词"
            automatic_targeting = "自动定位组"
            if budget in md:
                print(f"{md} 包含了'{budget}'")
                update_sp_ad_budget(country, md)
            elif targeting_group in md:
                print(f"{md} 包含了'{targeting_group}'")
                pass
                update_sp_ad_placement(country, md)
            elif search_term_good in md:
                print(f"{md} 包含了'{search_term_good}'")
                add_sp_ad_searchTerm_keyword(country, md)
            elif search_term_bad in md:
                print(f"{md} 包含了'{search_term_bad}'")
                add_sp_ad_searchTerm_negative_keyword(country, md)
            elif sku in md:
                print(f"{md} 包含了'{sku}'")
                update_sp_ad_sku(country, md)
            elif keyword in md:
                print(f"{md} 包含了'{keyword}'")
                update_sp_ad_keyword(country, md)
            elif automatic_targeting in md:
                print(f"{md} 包含了'{automatic_targeting}'")
                #update_sp_ad_automatic_targeting(country, md)
        pass

#自动劣质搜索词
