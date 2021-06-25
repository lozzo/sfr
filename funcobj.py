from functools import wraps
from typing import TypeVar, Callable, List, Union
from sfr.container import Container
# from exceptions import AnnotationsException
from pyspark.sql import SparkSession, DataFrame
# _T = TypeVar("_T")


class FnRunner(object):
    def run(self) -> str:
        fn: Callable[..., str] = self.fn
        args = self._get_fn_args()
        return fn(**args)

    def _get_fn_args(self) -> dict:
        try:
            fn_args = self.fn.__annotations__
            del fn_args["return"]
            for k, v in fn_args.items():
                fn_args.update({k: Container.get(v)})
            return fn_args
        except Exception as E:
            print(f"{self.name}:get args err")


class _FuncObj(object):
    def __init__(
            self, name: str, fn: Callable[..., str],
            depend_temp_view: List[Union[str, Callable[..., str]]]) -> None:
        """指标或者指标依赖函数的对象基类

        Args:
            name (str): 设定的特殊唯一名称或者fn的函数名称
            fn (Callable[..., str]): 实际执行的函数，其必须保证其参数为Container内注册过的对象，且必须返回一个字符串，这个字符串用来做spark的缓存
            depend_temp_view (List[Union[str, Callable[..., str]]]): 其所依赖的函数，必须是add_index_depend装饰器装饰了的函数，
                名称或者函数（如果没设置名称）本身，
        """
        self.name = name
        self.fn = fn
        self.depend_temp_view = depend_temp_view

    def __hash__(self) -> int:
        return hash(self.fn)


class IndexFuncObj(_FuncObj, FnRunner):
    def __init__(
            self, *, name: str, fn: Callable[..., str],
            depend_temp_view: List[Union[str, Callable[..., str]]]) -> None:
        super().__init__(name, fn, depend_temp_view)


class DependFuncObj(_FuncObj, FnRunner):
    def __init__(self, *, name: str, fn: Callable[..., str],
                 depend_temp_view: List[Union[str, Callable[..., str]]],
                 cache: Union[str, bool]) -> None:
        """那些被依赖的函数的对象
        Args:
            cache (Union[str, bool]): 如果是'auto'，则表示为自动缓存，bool则表示强制的缓存设置
        """
        super().__init__(name, fn, depend_temp_view)
        self.cache = cache
        self.called = False

    def run(self):
        if not self.called:
            temp_view_name = super().run()
            self.called = True
            if self.cache is True:
                pass
                # ss = Container.get(SparkSession)
                # ss
        else:
            print(f"依赖项:{self.name}已被执行过，不在重复执行")
