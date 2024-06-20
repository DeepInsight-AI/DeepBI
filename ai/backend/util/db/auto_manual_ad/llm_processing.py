import asyncio
import os

from AIChat import AIChat
from ai.backend.util.db.auto_yzj.utils.find import find_files
from ai.backend.util.db.auto_yzj.utils.trans_to import md_to_str, save_csv


def llm_processing(cur_time: str, country: str):
    # 替换为你的项目目录路径
    sql_files = find_files(directory='./', suffix='.sql')
    for sql_file_path in sql_files:
        csv_file_path = sql_file_path[0:-3:] + "csv"
        try:
            chat = AIChat(u_name="Bob", db_id=None, input_path=csv_file_path)
            dir_path = os.path.dirname(csv_file_path)
            # 从提问策略下找到所有的.md文件
            mds = find_files(directory=R"" + dir_path + "/提问策略/", suffix='.md')
            for md in mds:
                q_str = md_to_str(md)
                absolute_path = os.path.abspath(md)
                output_path = R"" + absolute_path[0:-2] + 'csv'
                asyncio.get_event_loop().run_until_complete(
                    chat.ask_question(q_str + "。请将执行得到的结果保存在" + R"" + output_path))
        except Exception as e:
            print(f"处理 {csv_file_path} 时发生错误: {e}")
