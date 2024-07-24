from typing import Any, Dict, List, Union

class ChatMemoryManager:
    """A class to manage the global memory including messages, grounding_sources, etc. on chat level"""

    def __init__(self, name: str = None, memory_pool: Dict = None):
        """
        This ChatMemoryManager can not be applied to grounding_source_pool in database mode.
        """
        self.name = name
        if memory_pool is None:
            memory_pool = {}
            self.memory_pool = memory_pool
        else:
            raise ValueError("Unknown backend option: {}".format(self.backend))


def get_pool_info_with_id(
        self,
        user_id: str,
        default_value: Union[List, Dict],
) -> Any:
    """Gets the information with user_id and chat_id from the pool."""
    pool = self.memory_pool
    if user_id in pool:
        return pool[user_id]
    else:
        return default_value


def set_pool_info_with_id(self, user_id: str, info: Any) -> None:
    """Sets the information with user_id and chat_id to the pool."""
    pool = self.memory_pool
    if user_id not in pool:
        pool[user_id] = {}
    pool[user_id] = info


def __iter__(self):
    """Iterates over the memory pool."""
    for user_id, info in self.memory_pool.items():
        yield user_id, info


def drop_item_with_id(self, user_id: str, chat_id: str):
    # drop item under one user
    if user_id in self.memory_pool:
        self.memory_pool[user_id].pop([chat_id], None)
