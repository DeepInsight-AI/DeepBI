# Define different agents with different ai models such as :DeepInsight
#                                       or proxy such as: DeepThought (use DeepInsight )
# The default key cannot be deleted but can be modified
#     default agent: api_check
# The default Settings are derived from DeepBI setting， model can be modified
#       default model setting at oai/xx.py
AGENT_LLM_MODEL = {
    "default": {
        "model": None  # base on DeepBI setting
    },
    "planner_user": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "mysql_engineer": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "postgresql_engineer": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "chart_presenter": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "mongodb_engineer": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "base_mysql_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "base_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "base_csv_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "base_postgresql_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "database_describer": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "Executor": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "task_selector": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "task_planner": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "data_planner": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "chart_planner": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "Analyst": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "python_executor": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "csv_echart_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "mysql_echart_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "postgresql_echart_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "starrocks_echart_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "mongodb_echart_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "mysql_matplotlib_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "mongodb_matplotlib_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    },
    "data_checker_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False
    }
}
