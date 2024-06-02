from preprocessing_data import preprocess_data
from llm_processing import llm_processing

if __name__ == "__main__":
    # for country in all_countrys:
    # 输入的是昨天的值，例如：今天是5-31，应该输入的是5-30
    cur_time = "2024-5-25"
    countrys = ['FR', 'ES', 'IT']

    # 预处理数据，生成相应的csv,初次运行会生成json文件，用来描述各个字段的意思
    preprocess_data(cur_time, countrys[0])
    llm_processing(cur_time, countrys[0])
