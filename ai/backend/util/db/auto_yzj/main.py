from preprocessing_data import preprocess_data,preprocess_sp_data
from llm_processing import llm_processing,llm_processing1,processing
from auto_execute import auto_execute
from preprocess_csv import preprocess_csv
from datetime import datetime
import time


def run():
    countrys = ['IT', 'ES']
    index = 1
    while True:
        today = datetime.today()
        #cur_time = today.strftime('%Y-%m-%d')
        cur_time = '2024-06-21'
        current_country = countrys[index]

        # 预处理数据，生成相应的csv,初次运行会生成json文件，用来描述各个字段的意思
        preprocess_data(cur_time, current_country)
        processing(cur_time, current_country, use_llm=False)
        llm_processing(cur_time, current_country, llm='default')  # deepseek
        # preprocess_sp_data(cur_time, countrys[1])
        # llm_processing1(cur_time, countrys[1])
        preprocess_csv(cur_time, current_country)
        auto_execute(cur_time, current_country)


        # 切换到下一个国家
        index = (index + 1) % len(countrys)

        time.sleep(60 * 60 * 24*3)



if __name__ == "__main__":
    run()

