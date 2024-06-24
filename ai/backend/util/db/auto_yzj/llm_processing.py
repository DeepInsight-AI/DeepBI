import asyncio
import os
import shutil

from AIChat import AIChat
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
#from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.搜索词优化.处理代码.优质搜索词 import main as main9
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.搜索词优化.处理代码.劣质搜索词 import main as main10
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.搜索词优化.处理代码.劣质搜索词 import main as main11
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.SKU优化.处理代码.关闭SKU import main as main12
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.SKU优化.处理代码.关闭SKU import main as main13

def processing(cur_time: str, country: str, use_llm: bool):
    if not use_llm:
        destination_dir = os.path.join("./日常优化/输出结果/", f"{country}_{cur_time}")
        os.makedirs(destination_dir, exist_ok=True)
        main1(destination_dir, cur_time, country)
        main2(destination_dir, cur_time, country)
        main3(destination_dir, cur_time, country)
        main4(destination_dir, cur_time, country)
        main5(destination_dir, cur_time, country)
        main6(destination_dir, cur_time, country)
        main7(destination_dir, cur_time, country)
        main8(destination_dir, cur_time, country)
        #main9(destination_dir, cur_time, country)
        main10(destination_dir, cur_time, country)
        main11(destination_dir, cur_time, country)
        main12(destination_dir, cur_time, country)
        main13(destination_dir, cur_time, country)

    else:
        pass

def llm_processing(cur_time: str, country: str, llm: str, version: int = 1):
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
                    output_path = R"" + absolute_path[0:-3] + '_' + country + '_' + cur_time + '_deepseek' + '.csv'
                else:
                    chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path, use_deepseek_config=False)
                    output_path = R"" + absolute_path[0:-3] + '_' + country + '_' + cur_time + '.csv'
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

def llm_processing1(cur_time: str, country: str, version: int = 1):
    # 替换为你的项目目录路径
    csv_files = find_file_by_name(directory='./日常优化/异常定位检测/', filename='预处理1.csv')
    print(csv_files)
    for csv_file_path in csv_files:
        try:
            chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path)
            dir_path = os.path.dirname(csv_file_path)
            # 从提问策略下找到所有的.md文件
            mds = find_files(directory=R"" + dir_path + "/提问策略/", suffix='1.md')
            for md in mds:
                q_str = md_to_str(md)
                absolute_path = os.path.abspath(md)
                output_path = R"" + absolute_path[0:-3] + '_' + country + '_' + cur_time + '.csv'
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
