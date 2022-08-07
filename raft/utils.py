import random
import subprocess
import threading


def randrangef(start: float, stop: float) -> float:
    return random.random() * (stop - start) + start
    r = random.random()
    return r * stop + (1 - r) * start


def build_loopback_ip() -> str:
    return "127.0.0.1"


def build_local_ip() -> str:
    ps_ifconfig = subprocess.Popen(["ifconfig"], stdout=subprocess.PIPE)
    ps_grep = subprocess.Popen(
        ["grep", "inet "], stdin=ps_ifconfig.stdout, stdout=subprocess.PIPE
    )
    output = subprocess.check_output(("grep", "-v", "127.0.0.1"), stdin=ps_grep.stdout)
    ps_ifconfig.wait()
    ps_grep.wait()

    if outputs := output.decode("utf-8").strip().split("\n"):
        # ["inet 255.255.255.255 netmask 0xffff0000 broadcast 255.255.255.255"]
        return outputs[0].split()[1]
    return "127.0.0.1"


class AtomicInteger:
    def __init__(self, value: int = 0):
        self.__value = value
        self.__lock = threading.Lock()

    def increase(self, value: int = 1) -> "AtomicInteger":
        with self.__lock:
            self.__value += value
        return self

    def decrease(self, value: int = 1) -> "AtomicInteger":
        with self.__lock:
            self.__value -= value
        return self

    def set(self, value: int = 0) -> "AtomicInteger":
        with self.__lock:
            self.__value = value
        return self

    @property
    def value(self) -> int:
        return self.__value
