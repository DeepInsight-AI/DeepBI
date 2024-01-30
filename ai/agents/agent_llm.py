"""
# Define different agents with different ai models
# The default key cannot be deleted but can be modified
#     default agent: api_check
#
#   llm:   such as :DeepInsight or proxy such as: DeepThought (use DeepInsight )
#           The default Settings are derived from DeepBI setting， model can be modified
#           default model setting at oai/xx.py
#   model: such as gpt4
#   replace_default: Whether to enable model replacement
#   use_message_count: Allows replacement Model to use the maximum number of communications
                0 all
                1 times
                2 times
"""
AGENT_LLM_MODEL = {
    "default": {
        "model": None  # base on DeepBI setting
    },
    "planner_user": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "mysql_engineer": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "postgresql_engineer": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "chart_presenter": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "mongodb_engineer": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "base_mysql_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "base_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "base_csv_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "base_postgresql_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "database_describer": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "Executor": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "task_selector": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "task_planner": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "data_planner": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "chart_planner": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "Analyst": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "python_executor": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "csv_echart_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "mysql_echart_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "postgresql_echart_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "starrocks_echart_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "mongodb_echart_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "mysql_matplotlib_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "mongodb_matplotlib_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    },
    "data_checker_assistant": {
        "llm": "DeepInsight",
        "model": "gpt-4-1106-preview",
        "replace_default": False,
        "use_message_count": 1
    }
}
