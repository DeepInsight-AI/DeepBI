from ai.backend.util.db.auto_yzj.utils.find import find_files,find_file_by_name
from ai.backend.util.db.auto_process.update_sp_ad_auto import auto_api as auto_api_old #update_sp_ad_keyword,update_sp_ad_budget,update_sp_ad_placement,add_sp_ad_searchTerm_keyword,add_sp_ad_searchTerm_negative_keyword,update_sp_ad_sku,update_sp_ad_automatic_targeting
from ai.backend.util.db.auto_process.update_sp_ad_auto_new import auto_api
from ai.backend.util.db.auto_process.update_sd_ad_auto import auto_api_sd


def auto_execute(cur_time: str, country: str,brand: str, strategy: str, version: int = 1):
    if strategy == 'daily':
        directory = "./日常优化/输出结果/"
    elif strategy == 'overstock':
        directory = "./滞销品优化/输出结果/"
    else:
        raise ValueError("Invalid strategy provided.")
    suffix = brand + '_' + country + '_' + cur_time + '.csv'
    #suffix = '自动_劣质广告活动_IT_2024-06-19.csv'
    mds = find_files(directory=directory, suffix=suffix)
    api = auto_api(brand,country)
    api1 = auto_api_sd(brand,country)
    print(mds)
    if len(mds) > 0:
        for md in mds:
            budget = "广告活动"
            targeting_group = "广告位"
            search_term_good = "优质搜索词"
            search_term_bad = "劣质搜索词"
            sku = "关闭SKU"
            sp_keyword = "特殊关键词"
            sp_automatic_targeting = "特殊自动定位组"
            sp_product_targets = "特殊商品投放"
            keyword = "关键词"
            automatic_targeting = "定位组"
            product_targets = "商品投放"
            product_targets_search_term_good = '优质_ASIN_搜索词'
            product_targets_search_term_bad = '劣质_ASIN_搜索词'
            auto_search_term_good = '优质自动搜索词'
            sd_sku = "关闭sdSKU"
            sd_budget = "广告sd活动"
            sd_product_targets = "商品sd投放"
            if budget in md:
                print(f"{md} 包含了'{budget}'")
                api.update_sp_ad_budget(country, md)
            elif sp_keyword in md:
                print(f"{md} 包含了'{sp_keyword}'")
            elif sp_automatic_targeting in md:
                print(f"{md} 包含了'{sp_automatic_targeting}'")
            elif sp_product_targets in md:
                print(f"{md} 包含了'{sp_product_targets}'")
            elif targeting_group in md:
                print(f"{md} 包含了'{targeting_group}'")
                api.update_sp_ad_placement(country, md)
            elif search_term_good in md:
                print(f"{md} 包含了'{search_term_good}'")
                api.add_sp_ad_searchTerm_keyword(country, md)
            elif auto_search_term_good in md:
                print(f"{md} 包含了'{auto_search_term_good}'")
                api.add_sp_ad_auto_searchTerm_keyword(country, md)
            elif search_term_bad in md:
                print(f"{md} 包含了'{search_term_bad}'")
                api.add_sp_ad_searchTerm_negative_keyword(country, md)
            elif sku in md:
                print(f"{md} 包含了'{sku}'")
                api.update_sp_ad_sku(country, md)
            elif keyword in md:
                print(f"{md} 包含了'{keyword}'")
                api.update_sp_ad_keyword(country, md)
            elif automatic_targeting in md:
                print(f"{md} 包含了'{automatic_targeting}'")
                api.update_sp_ad_automatic_targeting(country, md)
            elif product_targets in md:
                print(f"{md} 包含了'{product_targets}'")
                api.update_sp_ad_product_targets(country, md)
            elif product_targets_search_term_good in md:
                print(f"{md} 包含了'{product_targets_search_term_good}'")
                api.add_sp_ad_searchTerm_product(country, md)
            elif product_targets_search_term_bad in md:
                print(f"{md} 包含了'{product_targets_search_term_bad}'")
                api.add_sp_ad_negative_searchTerm_product(country, md)
            elif sd_sku in md:
                print(f"{md} 包含了'{sd_sku}'")
                api1.update_sd_ad_sku(country, md)
            elif sd_budget in md:
                print(f"{md} 包含了'{sd_budget}'")
                api1.update_sd_ad_budget(country, md)
            elif sd_product_targets in md:
                print(f"{md} 包含了'{sd_product_targets}'")
                api1.update_sd_ad_product_targets(country, md)
        pass


def auto_execute1(cur_time: str, country: str,brand: str, strategy: str, version: int = 1):
    if strategy == 'daily':
        directory = "./日常优化/输出结果/"
    elif strategy == 'overstock':
        directory = "./滞销品优化/输出结果/"
    else:
        raise ValueError("Invalid strategy provided.")
    suffix = brand + '_' + country + '_' + cur_time + '.csv'
    #suffix = '自动_劣质广告活动_IT_2024-06-19.csv'
    mds = find_files(directory=directory, suffix=suffix)
    api = auto_api(brand,country)
    api1 = auto_api_sd(brand,country)
    print(mds)
    if len(mds) > 0:
        for md in mds:
            budget = "广告活动"
            targeting_group = "广告位"
            search_term_good = "优质搜索词"
            search_term_bad = "劣质搜索词"
            sku = "关闭SKU"
            sp_keyword = "特殊关键词"
            sp_automatic_targeting = "特殊自动定位组"
            sp_product_targets = "特殊商品投放"
            keyword = "关键词"
            automatic_targeting = "定位组"
            product_targets = "商品投放"
            product_targets_search_term_good = 'ASIN_搜索词'
            auto_search_term_good = '优质自动搜索词'
            sd_sku = "关闭sdSKU"
            sd_budget = "广告sd活动"
            # aaa = '特殊关键词'
            # aaaa = '特殊商品投放'
            # aaaaa = '特殊定位组'
            if budget in md:
                print(f"{md} 包含了'{budget}'")
                api.update_sp_ad_budget(country, md)
            elif sp_keyword in md:
                print(f"{md} 包含了'{sp_keyword}'")
            elif sp_automatic_targeting in md:
                print(f"{md} 包含了'{sp_automatic_targeting}'")
            elif sp_product_targets in md:
                print(f"{md} 包含了'{sp_product_targets}'")
            elif targeting_group in md:
                print(f"{md} 包含了'{targeting_group}'")
                api.update_sp_ad_placement(country, md)
            elif search_term_good in md:
                print(f"{md} 包含了'{search_term_good}'")
                api.add_sp_ad_searchTerm_keyword(country, md)
            elif auto_search_term_good in md:
                print(f"{md} 包含了'{auto_search_term_good}'")
                api.add_sp_ad_auto_searchTerm_keyword(country, md)
            elif search_term_bad in md:
                print(f"{md} 包含了'{search_term_bad}'")
                #api.add_sp_ad_searchTerm_negative_keyword(country, md)
            elif sku in md:
                print(f"{md} 包含了'{sku}'")
                api.update_sp_ad_sku(country, md)
            elif keyword in md:
                print(f"{md} 包含了'{keyword}'")
                api.update_sp_ad_keyword(country, md)
            elif automatic_targeting in md:
                print(f"{md} 包含了'{automatic_targeting}'")
                api.update_sp_ad_automatic_targeting(country, md)
            elif product_targets in md:
                print(f"{md} 包含了'{product_targets}'")
                api.update_sp_ad_automatic_targeting(country, md)
            elif product_targets_search_term_good in md:
                print(f"{md} 包含了'{product_targets_search_term_good}'")
                api.add_sp_ad_searchTerm_product(country, md)
            elif sd_sku in md:
                print(f"{md} 包含了'{sd_sku}'")
                api1.update_sd_ad_sku(country, md)
            elif sd_budget in md:
                print(f"{md} 包含了'{sd_budget}'")
                api1.update_sd_ad_budget(country, md)
            # elif aaaaa in md:
            #     print(f"{md} 包含了'{aaaaa}'")
            #     api.update_sp_ad_automatic_targeting(country, md)
        pass

def auto_execute2(cur_time: str, country: str,brand: str, strategy: str, version: int = 1):
    if strategy == 'daily':
        directory = "./日常优化/输出结果/"
    elif strategy == 'overstock':
        directory = "./滞销品优化/输出结果/"
    else:
        raise ValueError("Invalid strategy provided.")
    suffix = brand + '_' + country + '_' + cur_time + '.csv'
    #suffix = '自动_劣质广告活动_IT_2024-06-19.csv'
    mds = find_files(directory=directory, suffix=suffix)
    api = auto_api(brand,country)
    api1 = auto_api_sd(brand,country)
    print(mds)
    if len(mds) > 0:
        for md in mds:
            budget = "优质广告活动"
            targeting_group = "优质广告位"
            search_term_good = "优质搜索词"
            search_term_bad = "劣质搜索词"
            sku = "关闭SKU"
            keyword = "优质关键词"
            automatic_targeting = "优质定位组"
            product_targets = "优质商品投放"
            product_targets_search_term_good = '优质_ASIN_搜索词'
            sd_budget = "优质广告sd活动"
            aaa = '特殊关键词'
            aaaa = '特殊商品投放'
            aaaaa = '特殊定位组'
            if budget in md:
                print(f"{md} 包含了'{budget}'")
                api.update_sp_ad_budget(country, md)
            elif targeting_group in md:
                print(f"{md} 包含了'{targeting_group}'")
                api.update_sp_ad_placement(country, md)
            elif search_term_good in md:
                print(f"{md} 包含了'{search_term_good}'")
                api.add_sp_ad_searchTerm_keyword(country, md)
            elif search_term_bad in md:
                print(f"{md} 包含了'{search_term_bad}'")
                #api.add_sp_ad_searchTerm_negative_keyword(country, md)
            elif sku in md:
                print(f"{md} 包含了'{sku}'")
                #api.update_sp_ad_sku(country, md)
            elif keyword in md:
                print(f"{md} 包含了'{keyword}'")
                api.update_sp_ad_keyword(country, md)
            elif automatic_targeting in md:
                print(f"{md} 包含了'{automatic_targeting}'")
                api.update_sp_ad_automatic_targeting(country, md)
            elif product_targets in md:
                print(f"{md} 包含了'{product_targets}'")
                api.update_sp_ad_automatic_targeting(country, md)
            elif product_targets_search_term_good in md:
                print(f"{md} 包含了'{product_targets_search_term_good}'")
                api.add_sp_ad_searchTerm_product(country, md)
            elif sd_budget in md:
                print(f"{md} 包含了'{sd_budget}'")
                api1.update_sd_ad_budget(country, md)
            # elif aaa in md:
            #     print(f"{md} 包含了'{aaa}'")
            #     api.update_sp_ad_keyword(country, md)
            # elif aaaa in md:
            #     print(f"{md} 包含了'{aaaa}'")
            #     api.update_sp_ad_keyword(country, md)
            # elif aaaaa in md:
            #     print(f"{md} 包含了'{aaaaa}'")
            #     api.update_sp_ad_automatic_targeting(country, md)
        pass


def auto_execute3(cur_time: str, country: str, brand: str, strategy: str, db: str, user='test', version: int = 1):
    if strategy == 'daily':
        directory = "./日常优化/输出结果/"
    elif strategy == 'overstock':
        directory = "./滞销品优化/输出结果/"
    else:
        raise ValueError("Invalid strategy provided.")
    suffix = brand + '_' + country + '_' + cur_time + '.csv'
    #suffix = '自动_劣质广告活动_IT_2024-06-19.csv'
    mds = find_files(directory=directory, suffix=suffix)
    api = auto_api(db, brand, country, user)
    api1 = auto_api_sd(db, brand, country)
    print(mds)
    if len(mds) > 0:
        for md in mds:
            search_term_good = "优质搜索词"
            search_term_bad = "劣质搜索词"
            product_targets_search_term_good = '优质_ASIN_搜索词'
            product_targets_search_term_bad = '劣质_ASIN_搜索词'
            auto_search_term_good = '优质自动搜索词'
            if search_term_good in md:
                print(f"{md} 包含了'{search_term_good}'")
                api.add_sp_ad_searchTerm_keyword(country, md)
            elif auto_search_term_good in md:
                print(f"{md} 包含了'{auto_search_term_good}'")
                api.add_sp_ad_auto_searchTerm_keyword(country, md)
            elif search_term_bad in md:
                print(f"{md} 包含了'{search_term_bad}'")
                api.add_sp_ad_searchTerm_negative_keyword(country, md)
            elif product_targets_search_term_good in md:
                print(f"{md} 包含了'{product_targets_search_term_good}'")
                api.add_sp_ad_searchTerm_product(country, md)
            elif product_targets_search_term_bad in md:
                print(f"{md} 包含了'{product_targets_search_term_bad}'")
                api.add_sp_ad_negative_searchTerm_product(country, md)
        pass

def auto_execute_rollback(cur_time: str, country: str,brand: str, strategy: str, version: int = 1):
    if strategy == 'daily':
        directory = "./日常优化/输出结果/"
    elif strategy == 'overstock':
        directory = "./滞销品优化/输出结果/"
    else:
        raise ValueError("Invalid strategy provided.")
    suffix = brand + '_' + country + '_' + cur_time + '.csv'
    #suffix = '自动_劣质广告活动_IT_2024-06-19.csv'
    mds = find_files(directory=directory, suffix=suffix)
    api = auto_api(brand)
    print(mds)
    if len(mds) > 0:
        for md in mds:
            budget = "优质广告活动"
            targeting_group = "优质广告位"
            search_term_good = "优质搜索词"
            search_term_bad = "劣质搜索词"
            sku = "SKU"
            keyword = "优质关键词"
            automatic_targeting = "优质定位组"
            product_targets = "优质商品投放"
            product_targets_search_term_good = '优质_ASIN_搜索词'
            aaa = '特殊关键词'
            aaaa = '特殊商品投放'
            aaaaa = '特殊定位组'
            if budget in md:
                print(f"{md} 包含了'{budget}'")
                api.rollback_sp_ad_budget(country, md)
            elif targeting_group in md:
                print(f"{md} 包含了'{targeting_group}'")
                api.rollback_sp_ad_placement(country, md)
            elif search_term_good in md:
                print(f"{md} 包含了'{search_term_good}'")
            elif search_term_bad in md:
                print(f"{md} 包含了'{search_term_bad}'")
                #api.add_sp_ad_searchTerm_negative_keyword(country, md)
            elif sku in md:
                print(f"{md} 包含了'{sku}'")
                #api.update_sp_ad_sku(country, md)
            elif keyword in md:
                print(f"{md} 包含了'{keyword}'")
                api.rollback_sp_ad_keyword(country, md)
            elif automatic_targeting in md:
                print(f"{md} 包含了'{automatic_targeting}'")
                api.rollback_sp_ad_automatic_targeting(country, md)
            elif product_targets in md:
                print(f"{md} 包含了'{product_targets}'")
                api.rollback_sp_ad_automatic_targeting(country, md)
            elif product_targets_search_term_good in md:
                print(f"{md} 包含了'{product_targets_search_term_good}'")
            elif aaa in md:
                print(f"{md} 包含了'{aaa}'")
                api.rollback_sp_ad_keyword(country, md)
            elif aaaa in md:
                print(f"{md} 包含了'{aaaa}'")
                api.rollback_sp_ad_automatic_targeting(country, md)
            elif aaaaa in md:
                print(f"{md} 包含了'{aaaaa}'")
                api.rollback_sp_ad_automatic_targeting(country, md)
        pass
