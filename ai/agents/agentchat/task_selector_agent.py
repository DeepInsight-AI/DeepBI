import asyncio
from typing import Callable, Dict, List, Optional, Union
from ai.backend.util.write_log import logger
from .conversable_agent import ConversableAgent
from .agent import Agent
from ai.backend.base_config import CONFIG
import re


class TaskSelectorAgent(ConversableAgent):
    """(In preview) A class for generic conversable agents which can be configured as assistant or user proxy.

    After receiving each message, the agent will send a reply to the sender unless the msg is a termination msg.
    For example, AssistantAgent and UserProxyAgent are subclasses of this class,
    configured with different default settings.

    To modify auto reply, override `generate_reply` method.
    To disable/enable human response in every turn, set `human_input_mode` to "NEVER" or "ALWAYS".
    To modify the way to get human input, override `get_human_input` method.
    To modify the way to execute code blocks, single code block, or function call, override `execute_code_blocks`,
    `run_code`, and `execute_function` methods respectively.
    To customize the initial message when a conversation starts, override `generate_init_message` method.
    """

    DEFAULT_SYSTEM_MESSAGE = """You are a helpful AI assistant.
    Solve tasks using your coding and language skills.
    In the following cases, suggest python code (in a python coding block) or shell script (in a sh coding block) for the user to execute.
        1. When you need to collect info, use the code to output the info you need, for example, browse or search the web, download/read a file, print the content of a webpage or a file, get the current date/time, check the operating system. After sufficient info is printed and the task is ready to be solved based on your language skill, you can solve the task by yourself.
        2. When you need to perform some task with code, use the code to perform the task and output the result. Finish the task smartly.
    Solve the task step by step if you need to. If a plan is not provided, explain your plan first. Be clear which step uses code, and which step uses your language skill.
    When using code, you must indicate the script type in the code block. The user cannot provide any other feedback or perform any other action beyond executing the code you suggest. The user can't modify your code. So do not suggest incomplete code which requires users to modify. Don't use a code block if it's not intended to be executed by the user.
    If you want the user to save the code in a file before executing it, put # filename: <filename> inside the code block as the first line. Don't include multiple code blocks in one response. Do not ask users to copy and paste the result. Instead, use 'print' function for the output when relevant. Check the execution result returned by the user.
    If the result indicates there is an error, fix the error and output the code again. Suggest the full code instead of partial code or code changes. If the error can't be fixed or if the task is not solved even after the code is executed successfully, analyze the problem, revisit your assumption, collect additional info you need, and think of a different approach to try.
    When you find an answer, verify the answer carefully. Include verifiable evidence in your response if possible.
    Reply "TERMINATE" in the end when everything is done.
        """

    def __init__(
        self,
        name: str,
        system_message: Optional[str] = DEFAULT_SYSTEM_MESSAGE,
        llm_config: Optional[Union[Dict, bool]] = None,
        is_termination_msg: Optional[Callable[[Dict], bool]] = None,
        max_consecutive_auto_reply: Optional[int] = None,
        human_input_mode: Optional[str] = "NEVER",
        code_execution_config: Optional[Union[Dict, bool]] = False,
        openai_proxy: Optional[str] = None,
        use_cache: Optional[bool] = True,
        **kwargs,
    ):
        """
        Args:
            name (str): agent name.
            system_message (str): system message for the ChatCompletion inference.
                Please override this attribute if you want to reprogram the agent.
            llm_config (dict): llm inference configuration.
                Please refer to [Completion.create](/docs/reference/oai/completion#create)
                for available options.
            is_termination_msg (function): a function that takes a message in the form of a dictionary
                and returns a boolean value indicating if this received message is a termination message.
                The dict can contain the following keys: "content", "role", "name", "function_call".
            max_consecutive_auto_reply (int): the maximum number of consecutive auto replies.
                default to None (no limit provided, class attribute MAX_CONSECUTIVE_AUTO_REPLY will be used as the limit in this case).
                The limit only plays a role when human_input_mode is not "ALWAYS".
            **kwargs (dict): Please refer to other kwargs in
                [ConversableAgent](conversable_agent#__init__).
        """
        super().__init__(
            name,
            system_message,
            is_termination_msg,
            max_consecutive_auto_reply,
            human_input_mode,
            code_execution_config=code_execution_config,
            llm_config=llm_config,
            openai_proxy=openai_proxy,
            use_cache=use_cache,
            **kwargs,
        )

    async def generate_reply(
        self,
        messages: Optional[List[Dict]] = None,
        sender: Optional[Agent] = None,
        exclude: Optional[List[Callable]] = None,
    ) -> Union[str, Dict, None]:
        """Reply based on the conversation history and the sender.

        Either messages or sender must be provided.
        Register a reply_func with `None` as one trigger for it to be activated when `messages` is non-empty and `sender` is `None`.
        Use registered auto reply functions to generate replies.
        By default, the following functions are checked in order:
        1. check_termination_and_human_reply
        2. generate_function_call_reply
        3. generate_code_execution_reply
        4. generate_oai_reply
        Every function returns a tuple (final, reply).
        When a function returns final=False, the next function will be checked.
        So by default, termination and human reply will be checked first.
        If not terminating and human reply is skipped, execute function or code and return the result.
        AI replies are generated only when no code execution is performed.

        Args:
            messages: a list of messages in the conversation history.
            default_reply (str or dict): default reply.
            sender: sender of an Agent instance.
            exclude: a list of functions to exclude.

        Returns:
            str or dict or None: reply. None if no reply is generated.
        ---------------------------------------------------------------
        """

        if all((messages is None, sender is None)):
            error_msg = f"Either {messages=} or {sender=} must be provided."
            logger.error(error_msg)
            raise AssertionError(error_msg)

        if messages is None:
            messages = self._oai_messages[sender]
        # print("messages: ", messages)

        if messages[-1].get("role") == "function":
            message_content = messages[-1].get("content")
            # print("message :", message)
            # print(colored("*" * 80, "green"), flush=True)
            return message_content

        for reply_func_tuple in self._reply_func_list:
            reply_func = reply_func_tuple["reply_func"]
            if exclude and reply_func in exclude:
                continue
            if self._match_trigger(reply_func_tuple["trigger"], sender):
                # print("messages : ", messages)
                if asyncio.coroutines.iscoroutinefunction(reply_func):
                    final, reply = await reply_func(self, messages=messages, sender=sender,
                                                    config=reply_func_tuple["config"])
                else:
                    final, reply = reply_func(self, messages=messages, sender=sender,
                                              config=reply_func_tuple["config"])

                if final:
                    # ***** Suggested function Call: task_base *****
                    # Arguments:
                    # {"qustion_message":"\nWhat is the most common house layout in the dataset?"}
                    # **********************************************
                    print('messages[-1][content] :', messages[-1]['content'])

                    # suggest_function = {'role': 'assistant', 'content': None, 'function_call': {'name': 'task_base',
                    #                                                          'arguments': '{"qustion_message":"\\nWhat is the most common house layout in the dataset?"}'}}

                    # Check if reply is in agents_functions
                    is_func = False
                    for func in CONFIG.agents_functions:
                        if len(str(reply)) > 0 and func in str(reply):
                            is_func = True
                            suggest_function = {'role': 'assistant', 'content': None, 'function_call': {'name': func,
                                                                                                        'arguments': '{"qustion_message":"' + str(
                                                                                                            messages[
                                                                                                                -1][
                                                                                                                'content']) + '"}'}}
                            print('reply : ', reply)
                            # return reply
                            return suggest_function

                    if not is_func:
                        suggest_function = {'role': 'assistant', 'content': None,
                                            'function_call': {'name': CONFIG.default_agents_functions,
                                                              'arguments': '{"qustion_message":"' + str(
                                                                  messages[-1][
                                                                      'content']) + '"}'}}
                        print('reply : ', reply)
                        # return reply
                        return suggest_function
        # return messages
        return self._default_auto_reply
