import asyncio
import os
import shutil

from ai.backend.util.db.auto_yzj.AIChat import AIChat
from ai.backend.util.db.auto_yzj.utils.find import find_files, find_file_by_name
from ai.backend.util.db.auto_yzj.utils.trans_to import md_to_str, save_csv
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.关键词优化.处理代码.劣质关键词 import main as main1
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.关键词优化.处理代码.优质关键词 import main as main2
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.特殊关键词.处理代码.特殊关键词 import main as main3
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.预算优化.处理代码.优质广告活动 import main as main4
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.预算优化.处理代码.劣质广告活动 import main as main5
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.预算优化.处理代码.优质广告活动 import main as main6
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.预算优化.处理代码.劣质广告活动 import main as main7
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.搜索词优化.处理代码.优质搜索词 import main as main8
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.搜索词优化.处理代码.优质搜索词 import main as main9
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.搜索词优化.处理代码.劣质搜索词 import main as main10
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.搜索词优化.处理代码.劣质搜索词 import main as main11
# from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.SKU优化.处理代码.关闭SKU import main as main12
# from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.SKU优化.处理代码.关闭SKU import main as main13
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.广告位优化.处理代码.优质广告位 import main as main14
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.广告位优化.处理代码.优质广告位 import main as main15
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.广告位优化.处理代码.劣质广告位 import main as main16
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.广告位优化.处理代码.劣质广告位 import main as main17
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.自动定位组优化.处理代码.优质定位组 import main as main18
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.自动定位组优化.处理代码.劣质定位组 import main as main19
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.特殊自动定位组.处理代码.特殊定位组 import main as main20
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.搜索词优化.处理代码.优质搜索词 import main as main21
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.商品投放搜索词优化.处理代码.优质_ASIN_搜索词 import main as main22
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.商品投放搜索词优化.处理代码.优质_ASIN_搜索词 import main as main23
# from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.复开SKU.处理代码.复开SKU import main as main24
# from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.复开SKU.处理代码.复开SKU import main as main25
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.预算优化.处理代码.优质广告活动 import main as main26
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.预算优化.处理代码.劣质广告活动 import main as main27
# from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.关闭SKU.处理代码.关闭SKU import main as main28
# from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.复开SKU.处理代码.复开SKU import main as main29
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.关键词优化.处理代码.优质关键词 import main as main30
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.关键词优化.处理代码.劣质关键词 import main as main31
from ai.backend.util.db.auto_yzj.滞销品优化.自动sp广告.预算优化.处理代码.优质广告活动 import main as main32
from ai.backend.util.db.auto_yzj.滞销品优化.自动sp广告.预算优化.处理代码.劣质广告活动 import main as main33
# from ai.backend.util.db.auto_yzj.滞销品优化.自动sp广告.关闭SKU.处理代码.关闭SKU import main as main34
# from ai.backend.util.db.auto_yzj.滞销品优化.自动sp广告.复开SKU.处理代码.复开SKU import main as main35
from ai.backend.util.db.auto_yzj.滞销品优化.自动sp广告.自动定位组优化.处理代码.优质定位组 import main as main36
from ai.backend.util.db.auto_yzj.滞销品优化.自动sp广告.自动定位组优化.处理代码.劣质定位组 import main as main37
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.搜索词优化.处理代码.劣质搜索词 import main as main38
from ai.backend.util.db.auto_yzj.滞销品优化.自动sp广告.搜索词优化.处理代码.劣质搜索词 import main as main39
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.商品投放优化.处理代码.优质商品投放 import main as main40
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.商品投放优化.处理代码.劣质商品投放 import main as main41
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.广告位优化.处理代码.优质广告位 import main as main42
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.广告位优化.处理代码.劣质广告位 import main as main43
from ai.backend.util.db.auto_yzj.滞销品优化.自动sp广告.广告位优化.处理代码.劣质广告位 import main as main44
from ai.backend.util.db.auto_yzj.滞销品优化.自动sp广告.广告位优化.处理代码.劣质广告位 import main as main45
from ai.backend.util.db.auto_yzj.滞销品优化.自动sp广告.搜索词优化.处理代码.优质搜索词 import main as main46
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.商品投放优化.处理代码.优质商品投放 import main as main47
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.商品投放优化.处理代码.劣质商品投放 import main as main48
from ai.backend.util.db.auto_yzj.日常优化.sd广告.预算优化.处理代码.优质广告活动 import main as main49
from ai.backend.util.db.auto_yzj.日常优化.sd广告.预算优化.处理代码.劣质广告活动 import main as main50
from ai.backend.util.db.auto_yzj.日常优化.sd广告.关闭SKU.处理代码.关闭SKU import main as main51
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.商品投放搜索词优化.处理代码.优质_ASIN_搜索词 import main as main52
from ai.backend.util.db.auto_yzj.滞销品优化.手动sp广告.商品投放搜索词优化.处理代码.劣质_ASIN_搜索词 import main as main53
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.特殊商品投放.处理代码.特殊商品投放 import main as main54
from ai.backend.util.db.auto_yzj.日常优化.异常定位检测.广告活动.处理代码.花费异常 import main as main55
from ai.backend.util.db.auto_yzj.日常优化.异常定位检测.广告活动.处理代码.ACOS异常好 import main as main56
from ai.backend.util.db.auto_yzj.日常优化.异常定位检测.广告活动.处理代码.ACOS异常差 import main as main57
from ai.backend.util.db.auto_yzj.日常优化.异常定位检测.广告位.处理代码.异常广告位 import main as main58
from ai.backend.util.db.auto_yzj.日常优化.异常定位检测.商品.处理代码.异常商品 import main as main59
from ai.backend.util.db.auto_yzj.日常优化.sd广告.复开SKU.处理代码.复开SKU import main as main60
from ai.backend.util.db.auto_yzj.日常优化.sd广告.商品投放优化.处理代码.优质商品投放 import main as main61
from ai.backend.util.db.auto_yzj.日常优化.sd广告.商品投放优化.处理代码.劣质商品投放 import main as main62
from ai.backend.util.db.auto_yzj.日常优化.sd广告.特殊商品投放.处理代码.特殊商品投放 import main as main63
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.商品投放搜索词优化.处理代码.劣质_ASIN_搜索词 import main as main64


def processing(cur_time: str, country: str, use_llm: bool, brand: str, strategy: str, db:str):
    if not use_llm:
        if strategy == 'daily':
            destination_dir = os.path.join("./日常优化/输出结果/", f"{brand}_{country}_{cur_time}")
            os.makedirs(destination_dir, exist_ok=True)
            main1(destination_dir, brand, cur_time, country)
            main2(destination_dir, brand, cur_time, country)
            main3(destination_dir, brand, cur_time, country)
            main4(destination_dir, brand, cur_time, country)
            main5(destination_dir, brand, cur_time, country)
            main6(destination_dir, brand, cur_time, country)
            main7(destination_dir, brand, cur_time, country)
            main8(destination_dir, brand, cur_time, country)
            main9(destination_dir, brand, cur_time, country)
            main10(destination_dir, brand, cur_time, country)
            main11(destination_dir, brand, cur_time, country)
            main12(destination_dir, brand, cur_time, country)
            main13(destination_dir, brand, cur_time, country)
            main14(destination_dir, brand, cur_time, country)
            main15(destination_dir, brand, cur_time, country)
            main16(destination_dir, brand, cur_time, country)
            main17(destination_dir, brand, cur_time, country)
            main18(destination_dir, brand, cur_time, country)
            main19(destination_dir, brand, cur_time, country)
            main20(destination_dir, brand, cur_time, country)
            main22(destination_dir, brand, cur_time, country)
            main24(destination_dir, brand, cur_time, country)
            main25(destination_dir, brand, cur_time, country)
            main40(destination_dir, brand, cur_time, country)
            main41(destination_dir, brand, cur_time, country)
            main49(destination_dir, brand, cur_time, country)
            main50(destination_dir, brand, cur_time, country)
            main51(destination_dir, brand, cur_time, country)
            main54(destination_dir, brand, cur_time, country)
            main55(destination_dir, brand, cur_time, country)
            main56(destination_dir, brand, cur_time, country)
            main57(destination_dir, brand, cur_time, country)
            main58(destination_dir, brand, cur_time, country)
            main59(destination_dir, brand, cur_time, country)
            main60(destination_dir, brand, cur_time, country)
            main61(destination_dir, brand, cur_time, country)
            main62(destination_dir, brand, cur_time, country)
            main63(destination_dir, brand, cur_time, country)
            main64(destination_dir, brand, cur_time, country)

        elif strategy == 'overstock':
            destination_dir = os.path.join("./滞销品优化/输出结果/", f"{brand}_{country}_{cur_time}")
            os.makedirs(destination_dir, exist_ok=True)
            main21(destination_dir, brand, cur_time, country)
            # main23(destination_dir, brand, cur_time, country)
            main26(destination_dir, brand, cur_time, country)
            main27(destination_dir, brand, cur_time, country)
            main28(destination_dir, brand, cur_time, country)
            main29(destination_dir, brand, cur_time, country)
            main30(destination_dir, brand, cur_time, country)
            main31(destination_dir, brand, cur_time, country)
            main32(destination_dir, brand, cur_time, country)
            main33(destination_dir, brand, cur_time, country)
            main34(destination_dir, brand, cur_time, country)
            main35(destination_dir, brand, cur_time, country)
            main36(destination_dir, brand, cur_time, country)
            main37(destination_dir, brand, cur_time, country)
            main38(destination_dir, brand, cur_time, country)
            main39(destination_dir, brand, cur_time, country)
            main42(destination_dir, brand, cur_time, country)
            main43(destination_dir, brand, cur_time, country)
            main44(destination_dir, brand, cur_time, country)
            main45(destination_dir, brand, cur_time, country)
            main46(destination_dir, brand, cur_time, country)
            main47(destination_dir, brand, cur_time, country)
            main48(destination_dir, brand, cur_time, country)
            main52(destination_dir, brand, cur_time, country)
            main53(destination_dir, brand, cur_time, country)

    else:
        pass


def processing_test(cur_time: str, country: str, use_llm: bool, brand: str, strategy: str, db:str):
    if not use_llm:
        if strategy == 'daily':
            destination_dir = os.path.join("./日常优化/输出结果/", f"{brand}_{country}_{cur_time}")
            os.makedirs(destination_dir, exist_ok=True)
            # main1(destination_dir, brand, cur_time, country)
            # main2(destination_dir, brand, cur_time, country)
            # main3(destination_dir, brand, cur_time, country)
            # main4(destination_dir, brand, cur_time, country)
            # main5(destination_dir, brand, cur_time, country)
            # main6(destination_dir, brand, cur_time, country)
            # main7(destination_dir, brand, cur_time, country)
            main8(destination_dir, brand, cur_time, country, db)
            main9(destination_dir, brand, cur_time, country, db)
            main10(destination_dir, brand, cur_time, country, db)
            #main11(destination_dir, brand, cur_time, country)
            # main12(destination_dir, brand, cur_time, country)
            # main13(destination_dir, brand, cur_time, country)
            # main14(destination_dir, brand, cur_time, country)
            # main15(destination_dir, brand, cur_time, country)
            # main16(destination_dir, brand, cur_time, country)
            # main17(destination_dir, brand, cur_time, country)
            # main18(destination_dir, brand, cur_time, country)
            # main19(destination_dir, brand, cur_time, country)
            # main20(destination_dir, brand, cur_time, country)
            main22(destination_dir, brand, cur_time, country, db)
            # main23(destination_dir, brand, cur_time, country)
            # main24(destination_dir, brand, cur_time, country)
            # main25(destination_dir, brand, cur_time, country)
            # main40(destination_dir, brand, cur_time, country)
            # main41(destination_dir, brand, cur_time, country)
            # main49(destination_dir, brand, cur_time, country)
            # main50(destination_dir, brand, cur_time, country)
            # main51(destination_dir, brand, cur_time, country)
            # main55(destination_dir, brand, cur_time, country)
            # main56(destination_dir, brand, cur_time, country)
            # main57(destination_dir, brand, cur_time, country)
            # main58(destination_dir, brand, cur_time, country)
            # main59(destination_dir, brand, cur_time, country)
            # main60(destination_dir, brand, cur_time, country)
            # main61(destination_dir, brand, cur_time, country)
            # main62(destination_dir, brand, cur_time, country)
            # main63(destination_dir, brand, cur_time, country)
            main64(destination_dir, brand, cur_time, country, db)
        elif strategy == 'overstock':
            destination_dir = os.path.join("./滞销品优化/输出结果/", f"{brand}_{country}_{cur_time}")
            os.makedirs(destination_dir, exist_ok=True)
            # main21(destination_dir, brand, cur_time, country)
            # main23(destination_dir, brand, cur_time, country)
            # main26(destination_dir, brand, cur_time, country)
            main27(destination_dir, brand, cur_time, country)
            main28(destination_dir, brand, cur_time, country)
            main29(destination_dir, brand, cur_time, country)
            # main30(destination_dir, brand, cur_time, country)
            # main31(destination_dir, brand, cur_time, country)
            # main32(destination_dir, brand, cur_time, country)
            main33(destination_dir, brand, cur_time, country)
            main34(destination_dir, brand, cur_time, country)
            main35(destination_dir, brand, cur_time, country)
            # main36(destination_dir, brand, cur_time, country)
            # main37(destination_dir, brand, cur_time, country)
            # main38(destination_dir, brand, cur_time, country)
            # main39(destination_dir, brand, cur_time, country)

    else:
        pass

def llm_processing(cur_time: str, country: str, llm: str, brand: str, version: int = 1):
    # 替换为你的项目目录路径
    csv_files = find_file_by_name(directory='./日常优化/', filename='预处理.csv')
    print(csv_files)
    for csv_file_path in csv_files:
        try:
            dir_path = os.path.dirname(csv_file_path)
            # 从提问策略下找到所有的.md文件
            mds = find_files(directory=R"" + dir_path + "/提问策略/", suffix=f'_v1_{version}.md')
            #mds = find_files(directory=R"" + dir_path + "/提问策略/", suffix='劣质广告位_v1_1.md')
            print(mds)
            for md in mds:
                q_str = md_to_str(md)
                absolute_path = os.path.abspath(md)
                if llm == 'deepseek':
                    chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path, use_deepseek_config=True)
                    output_path = R"" + absolute_path[0:-3] + '_' + brand + '_' + country + '_' + cur_time + '_deepseek' + '.csv'
                else:
                    chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path, use_deepseek_config=False)
                    output_path = R"" + absolute_path[0:-3] + '_' + brand + '_' + country + '_' + cur_time + '.csv'
                while not os.path.exists(output_path):
                    q_str_with_extra_info = q_str + "请将执行得到的结果保存在" + R"" + output_path
                    asyncio.get_event_loop().run_until_complete(
                        chat.ask_question(q_str_with_extra_info))
                    # 在每次循环中在提示信息的末尾添加额外的内容
                    q_str += "。"
                # asyncio.get_event_loop().run_until_complete(
                #     chat.ask_question(q_str + "。请将执行得到的结果保存在" + R"" + output_path))
        except Exception as e:
            print(f"处理 {csv_file_path} 时发生错误: {e}")


def llm_processing2(cur_time: str, country: str, llm: str, brand: str, version: int = 2):
    # 替换为你的项目目录路径
    csv_files = find_file_by_name(directory='./滞销品优化/', filename='预处理.csv')
    print(csv_files)
    for csv_file_path in csv_files:
        try:
            dir_path = os.path.dirname(csv_file_path)
            # 从提问策略下找到所有的.md文件
            mds = find_files(directory=R"" + dir_path + "/提问策略/", suffix=f'_v1_{version}.md')
            #mds = find_files(directory=R"" + dir_path + "/提问策略/", suffix='劣质广告位_v1_1.md')
            print(mds)
            for md in mds:
                q_str = md_to_str(md)
                absolute_path = os.path.abspath(md)
                if llm == 'deepseek':
                    chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path, use_deepseek_config=True)
                    output_path = R"" + absolute_path[0:-3] + '_' + brand + '_' + country + '_' + cur_time + '_deepseek' + '.csv'
                else:
                    chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path, use_deepseek_config=False)
                    output_path = R"" + absolute_path[0:-3] + '_' + brand + '_' + country + '_' + cur_time + '.csv'
                while not os.path.exists(output_path):
                    q_str_with_extra_info = q_str + "请将执行得到的结果保存在" + R"" + output_path
                    asyncio.get_event_loop().run_until_complete(
                        chat.ask_question(q_str_with_extra_info))
                    # 在每次循环中在提示信息的末尾添加额外的内容
                    q_str += "。"
                # asyncio.get_event_loop().run_until_complete(
                #     chat.ask_question(q_str + "。请将执行得到的结果保存在" + R"" + output_path))
        except Exception as e:
            print(f"处理 {csv_file_path} 时发生错误: {e}")

def llm_processing1(cur_time: str, country: str, llm: str, brand: str, version: int = 0):
    # 替换为你的项目目录路径
    csv_files = find_file_by_name(directory='./日常优化/异常定位检测/', filename='预处理1.csv')
    print(csv_files)
    for csv_file_path in csv_files:
        try:
            chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path)
            dir_path = os.path.dirname(csv_file_path)
            # 从提问策略下找到所有的.md文件
            mds = find_files(directory=R"" + dir_path + "/提问策略/", suffix=f'1_v1_{version}.md')
            for md in mds:
                q_str = md_to_str(md)
                absolute_path = os.path.abspath(md)
                if llm == 'deepseek':
                    chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path, use_deepseek_config=True)
                    output_path = R"" + absolute_path[0:-3] + '_' + brand + '_' + country + '_' + cur_time + '_deepseek' + '.csv'
                else:
                    chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path, use_deepseek_config=False)
                    output_path = R"" + absolute_path[0:-3] + '_' + brand + '_' + country + '_' + cur_time + '.csv'
                # output_path = R"" + absolute_path[0:-3] + '_' + country + '_' + cur_time + '.csv'
                # output_path = R"" + absolute_path[0:-3] + '_' + country + '_' + cur_time + '_deepseek' + '.csv'
                while not os.path.exists(output_path):
                    q_str_with_extra_info = q_str + "请将执行得到的结果保存在" + R"" + output_path
                    asyncio.get_event_loop().run_until_complete(
                        chat.ask_question(q_str_with_extra_info))
                    # 在每次循环中在提示信息的末尾添加额外的内容
                    q_str += "。"
                # asyncio.get_event_loop().run_until_complete(
                #     chat.ask_question(q_str + "。请将执行得到的结果保存在" + R"" + output_path))

        except Exception as e:
            print(f"处理 {csv_file_path} 时发生错误: {e}")


def llm_processing3(cur_time: str, country: str, llm: str, brand: str, version: int = 1):
    # 替换为你的项目目录路径
    csv_files = find_file_by_name(directory='./日常优化/异常定位检测/', filename='预处理.csv')
    print(csv_files)
    for csv_file_path in csv_files:
        try:
            dir_path = os.path.dirname(csv_file_path)
            # 从提问策略下找到所有的.md文件
            mds = find_files(directory=R"" + dir_path + "/提问策略/", suffix=f'_v1_{version}.md')
            #mds = find_files(directory=R"" + dir_path + "/提问策略/", suffix='劣质广告位_v1_1.md')
            print(mds)
            for md in mds:
                q_str = md_to_str(md)
                absolute_path = os.path.abspath(md)
                if llm == 'deepseek':
                    chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path, use_deepseek_config=True)
                    output_path = R"" + absolute_path[0:-3] + '_' + brand + '_' + country + '_' + cur_time + '_deepseek' + '.csv'
                else:
                    chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path, use_deepseek_config=False)
                    output_path = R"" + absolute_path[0:-3] + '_' + brand + '_' + country + '_' + cur_time + '.csv'
                while not os.path.exists(output_path):
                    q_str_with_extra_info = q_str + "请将执行得到的结果保存在" + R"" + output_path
                    asyncio.get_event_loop().run_until_complete(
                        chat.ask_question(q_str_with_extra_info))
                    # 在每次循环中在提示信息的末尾添加额外的内容
                    q_str += "。"
                # asyncio.get_event_loop().run_until_complete(
                #     chat.ask_question(q_str + "。请将执行得到的结果保存在" + R"" + output_path))
        except Exception as e:
            print(f"处理 {csv_file_path} 时发生错误: {e}")
