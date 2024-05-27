"""
# Define different agents with different ai models
       The default key cannot be deleted but can be modified
       default agent: api_check not in AGENT_LLM_MODEL will use default setting

   llm:   such as :DeepInsight or proxy such as: DeepThought (use DeepInsight )
           The default Settings are derived from DeepBI setting， model can be modified
           default model setting at oai/xx.py

   model: such as gpt4

   replace_default: Whether to enable model replacement

   use_message_count: Allows replacement Model to use the maximum number of communications
                0 all
                1 times
                2 times
   notice:
        DeepInsight
        Determine the model apikey Settings and make them correct
        
        DeepInsight
"""
AGENT_LLM_MODEL = {
    "planner_user": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "mysql_engineer": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "postgresql_engineer": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "chart_presenter": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "mongodb_engineer": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "base_mysql_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "base_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "base_csv_assistant": {
        "llm": "Deepseek",
        "model": "glm-4",
        "replace_default": False,
        "use_message_count": 0
    },
    "base_postgresql_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "database_describer": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "Executor": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "task_selector": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "task_planner": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "data_planner": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "chart_planner": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "Analyst": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "python_executor": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "csv_echart_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "mysql_echart_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "postgresql_echart_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "starrocks_echart_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "mongodb_echart_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "mysql_matplotlib_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "mongodb_matplotlib_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "data_checker_assistant": {
        "llm": "Deepseek",
        "model": "deepseek-coder",
        "replace_default": False,
        "use_message_count": 0
    },
    "select_analysis_assistant": {
        "llm": "Deepseek",
        "model": "glm-4",
        "replace_default": False,
        "use_message_count": 0
    },
    "Admin": {
        "llm": "Deepseek",
        "model": "glm-4",
        "replace_default": False,
        "use_message_count": 0
    }
}
