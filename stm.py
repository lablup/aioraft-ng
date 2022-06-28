import abc
import asyncio
from contextlib import suppress


class FiniteStateMachine(abc.ABC):
    pass


class RaftStateMachine(FiniteStateMachine):
    def __init__(self):
        self._state = FollowerState()
        self._state_task = asyncio.create_task(self.state.run())

    async def run(self):
        asyncio.sleep(3.0)

    def transition(self, next_state: "RaftState"):
        if task := self._state_task:
            with suppress(asyncio.CancelledError):
                task.cancel()
        self._state = next_state
        self._state_task = asyncio.create_task(self.state.run())

    @property
    def state(self) -> "RaftState":
        return self._state


class RaftState(abc.ABC):
    def __init__(self, state_machine: RaftStateMachine):
        self._state_machine = state_machine

    @abc.abstractmethod
    async def run(self):
        raise NotImplementedError()


class FollowerState(RaftState):
    def __init__(self, state_machine: RaftStateMachine):
        super().__init__(state_machine=state_machine)

    async def run(self):
        print(f'[{self.__class__.__name__}] run()')
        await asyncio.sleep(3.0)


class CandidateState(RaftState):
    def __init__(self, state_machine: RaftStateMachine):
        super().__init__(state_machine=state_machine)

    async def run(self):
        print(f'[{self.__class__.__name__}] run()')


class LeaderState(RaftState):
    def __init__(self, state_machine: RaftStateMachine):
        super().__init__(state_machine=state_machine)

    async def run(self):
        print(f'[{self.__class__.__name__}] run()')


def main():
    fsm = RaftStateMachine()    # noqa: F841


if __name__ == "__main__":
    main()
