from .conversable_agent import ConversableAgent
from .agent import Agent
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from ai.agents import oai
import asyncio


class CheckAgent(ConversableAgent):
    DEFAULT_SYSTEM_MESSAGE = """You are a helpful AI assistant.
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
            **kwargs,
        )

        self.register_reply(Agent, CheckAgent.generate_oai_reply)

    async def generate_oai_reply(
            self,
            messages: Optional[List[Dict]] = None,
            sender: Optional[Agent] = None,
            config: Optional[Any] = None,
    ) -> Tuple[bool, Union[str, Dict, None]]:
        """Generate a reply using autogen.oai.
        """

        llm_config = self.llm_config if config is None else config
        if llm_config is False:
            return False, None
        if messages is None:
            messages = self._oai_messages[sender]

        def create_chat():
            # print("create_chat: ")
            # TODO: #1143 handle token limit exceeded error
            if self.openai_proxy is None:
                response = oai.ChatCompletion.create(
                    context=messages[-1].pop("context", None), messages=self._oai_system_message + messages,
                    use_cache=False,
                    agent_name=self.name,
                    **llm_config
                )
            else:
                response = oai.ChatCompletion.create(
                    context=messages[-1].pop("context", None), messages=self._oai_system_message + messages,
                    use_cache=False,
                    openai_proxy=self.openai_proxy,
                    agent_name=self.name,
                    **llm_config
                )
            # print("response: ", response)
            return response

        async def consume_async():
            # loop = asyncio.new_event_loop()
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, create_chat)
            # print("result :", result)
            return result

        response = await consume_async()
        # print("response: ", response)

        # # TODO: #1143 handle token limit exceeded error
        # response = oai.ChatCompletion.create(
        #     context=messages[-1].pop("context", None), messages=self._oai_system_message + messages,
        #     agent_name=self.name, **llm_config
        # )

        return True, oai.ChatCompletion.extract_text_or_function_call(response)[0]
