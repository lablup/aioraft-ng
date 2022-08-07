import enum
from typing import Type, TypeVar

RaftId = str


class RaftState(enum.Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


T_aobj = TypeVar("T_aobj", bound="aobject")


class aobject(object):
    """
    An "asynchronous" object which guarantees to invoke both ``def __init__(self, ...)`` and
    ``async def __ainit(self)__`` to ensure asynchronous initialization of the object.

    You can create an instance of subclasses of aboject in the following way:

    .. code-block:: python

       o = await SomeAObj.new(...)
    """

    @classmethod
    async def new(cls: Type[T_aobj], *args, **kwargs) -> T_aobj:
        """
        We can do ``await SomeAObject(...)``, but this makes mypy
        to complain about its return type with ``await`` statement.
        This is a copy of ``__new__()`` to workaround it.
        """
        instance = super().__new__(cls)
        cls.__init__(instance, *args, **kwargs)
        await instance.__ainit__()
        return instance

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def __ainit__(self) -> None:
        """
        Automatically called when creating the instance using
        ``await SubclassOfAObject(...)``
        where the arguments are passed to ``__init__()`` as in
        the vanilla Python classes.
        """
        pass
