from sfr.container import Container
from sfr.exceptions import AnnotationsException
from sfr.funcobj import DependFuncObj, IndexFuncObj

from functools import wraps
from typing import Callable, Dict, List, Union

__all__ = ["IndexRunner", "index", "depend", "factory"]


class IndexRunner(object):
    _ins = None
    _index: Dict[str, IndexFuncObj] = {}
    _depend: Dict[str, DependFuncObj] = {}
    _index_prefix = "index_"
    _depend_prefix = "_depend"

    def __init__(self, *, auto_cache_number: int, debug=False) -> None:
        self.auto_cache_number = auto_cache_number
        self.debug = debug

    def __new__(cls, *args, **kwargs):
        if cls._ins is None:
            cls._ins = object.__new__(cls)
        return cls._ins

    @classmethod
    def add_index(cls,
                  *,
                  index_name: str,
                  depend_temp_view: List[Union[str, Callable[..., str]]] = []):
        """添加一个新的指标到上下文,被此装饰器装饰的指标函数必须返回一个字符串，及生成指标的的TempView的名称

        Args:
            index_name (str): 这个指标的中文名称，例如 '城市发展指数'
            depend_temp_view (List[Union[str,Callable[..., str]]], optional): 所需要的前置TempView列表. Defaults to [].
        """
        index_name = cls.get_index_name(index_name)
        depend_temp_view = [cls.get_depend_name(x) for x in depend_temp_view]

        def decorator(fn):
            cls._index.update({
                index_name:
                IndexFuncObj(name=index_name,
                             fn=fn,
                             depend_temp_view=depend_temp_view)
            })

            @wraps(fn)
            def inner(*args, **kwargs):
                raise Exception("指标注册后只能使用IndexRunner.run('指标名称')来运行")

            return inner

        return decorator

    @classmethod
    def get_index_name(cls, name):
        return f"{name}"

    @classmethod
    def get_depend_name(cls, obj: Union[str, Callable[..., str]]):
        return f"{obj if isinstance(obj,str) else obj.__qualname__}"

    @classmethod
    def add_index_depend(cls,
                         *,
                         name: Union[str, None] = None,
                         cache: Union[str, bool] = "auto",
                         depend_temp_view: List[Union[str,
                                                      Callable[...,
                                                               str]]] = []):
        """添加一个指标前置，及某个TempView被多个指标所依赖，则用此装饰器装饰，被此装饰器装饰的函数必须返
           回该函数创建的TempView的名称，需要注意的是，各依赖之间不能循环依赖，必须是一个有向无环图

        Args:
            name (Union[str,None]): 该指标依赖的名称，IndexRunner.add_index 或者 IndexRunner.add_index_depend 的参数 depend_temp_view 中被使用,
                如果为空则使用fn.__qualname__ Defaults to None
            cache (Union[str,bool]): 该TempView是否缓存，可选参数为'auto'则代表自动判断（在初始化对象的时候设置auto对应值），
                bool值表示是否. Defaults to 'auto'
            depend_temp_view (List[Union[str,Callable[..., str]]], optional): 所需要的前置TempView列表 Defaults to [].
        """

        depend_temp_view = [cls.get_depend_name(x) for x in depend_temp_view]

        def decorator(fn: Callable[..., str]):
            _name = cls.get_depend_name(name or fn)
            func_annotations = fn.__annotations__
            if func_annotations.get("return", type(None)) != str:
                raise Exception(f"depend fn {_name}:{fn} must return str")
            cls._depend.update({
                _name:
                DependFuncObj(name=_name,
                              fn=fn,
                              depend_temp_view=depend_temp_view,
                              cache=cache)
            })

            @wraps(fn)
            def inner(*args, **kwargs):
                raise Exception("依赖函数只能有IndexRunner.run来自动调用")

            return inner

        return decorator

    def run(self,
            *,
            index_name: Union[str, List[str], None] = None,
            sort: Union[str] = None):
        """开始运行指标，可以选择只运行某一些指标，默认是全运行

        Args:
            index_name (Union[str,List[str],None], optional): 选择需要运行的指标,如果为None，则运行所有指标，
                如果是字符串，则只运行这一个指标，如果是列表，则运行这一列. Defaults to None.
        """

        self._check_depend_func_in_cycle_depend()
        running_sequence = self._get_runing_sequence(index_name)
        # 配置cahche为auto的依赖的缓存
        dep_use_count = {}
        for i in running_sequence.values():
            for dep_func_obj in i:
                name = dep_func_obj.name
                count = dep_use_count.get(name, 0)
                count += 1
                dep_use_count.update({name: count})
        print(dep_use_count)
        for name, count in dep_use_count.items():
            if count > self.auto_cache_number:
                self._depend.get(name).cache = True

        # 开始执行
        for index_func_obj, dep_obj_list in running_sequence.items():
            print(f"开始执行{index_func_obj.name},首先执行依赖项")
            for dep_obj in dep_obj_list:
                print(f"开始执行{index_func_obj.name}的依赖项:{dep_obj.name}")
                dep_obj.run()
            print(f"依赖执行结束,开始执行{index_func_obj.name}")
            index_func_obj.run()

    def _check_depend_func_in_cycle_depend(self):
        """检查被IndexRunner.add_index_depend 装饰的之间是否存在循环依赖,如果存在循环依赖则抛出异常
        """
        self._get_depend_running_sequence(self._depend)

    def _get_depend_running_sequence(
            self, dep: Dict[str, DependFuncObj]) -> List[DependFuncObj]:
        """获取指标所依赖的计算的执行顺序

        Args:
            dep (Dict[str,DependFuncObj]): 需要用到的依赖项

        Raises:
            Exception: 当遇到循环依赖时，就会抛出异常

        Returns:
            [type]: 一个依赖执行的排序函数列表
        """
        in_degress = dict((u, 0) for u in dep.keys())
        num = len(in_degress)
        for i in dep:
            for v in dep[i].depend_temp_view:
                in_degress[v] += 1

        Q = [u for u in in_degress if in_degress[u] == 0]
        seq = []
        while Q:
            u = Q.pop()
            seq.append(u)
            for v in dep[u].depend_temp_view:
                in_degress[v] -= 1
                if in_degress[v] == 0:
                    Q.append(v)

        if len(seq) == num:
            return [self._depend.get(x) for x in seq[::-1]]
        else:
            cycle_need = [
                "".join(x.split(self._depend_prefix)[1::]) for x in seq
                if x != 0
            ]
            raise Exception(f"依赖之间存在循环依赖，存在循环依赖的为:{cycle_need}")
            # return True

    def _get_runing_sequence(
        self, index_name: Union[str, List[str], None]
    ) -> Dict[IndexFuncObj, List[DependFuncObj]]:
        """获取指标的运行顺序，将受依赖的指标进行进行先运行
        """
        # 拿到需要执行的指标
        index_funcs = list(self._index.values()) if index_name is None else [
            self._index.get(index_name, None)
        ] if not isinstance(index_name, list) else [
            self._index.get(x, None) for x in index_name
        ]
        running_list: Dict[IndexFuncObj, List[DependFuncObj]] = {}
        for i in index_funcs:
            dep_funcs = self._get_index_running_depend_seqience(i.name)
            running_list.update({i: dep_funcs})
        return running_list

    def _get_index_running_depend_seqience(
            self, index_name: str) -> List[Union[DependFuncObj, IndexFuncObj]]:
        """获取具体某一个指标的所有依赖并且排出执行顺序

        Args:
            index_name ([str]): 指标的名称
        """
        depend_func_name = self._get_index_depend_name(index_name)
        func_map = dict((x, self._depend.get(x)) for x in depend_func_name)
        return self._get_depend_running_sequence(func_map)

    def _get_index_depend_name(self, index_name) -> List[str]:
        """获取具体某一个指标所依赖的依赖项的名称

        Args:
            index_name ([type]): [description]

        Returns:
            [type]: [description]
        """
        index_name = self.get_index_name(index_name)
        index_obj = self._index.get(index_name)
        dep = index_obj.depend_temp_view
        for i in dep:
            _dep = self._depend.get(i).depend_temp_view
            for x in _dep:
                if x not in dep:
                    dep.append(x)
        return dep

    def check_index(self):
        """检查所有指标及其依赖是否按照要求进行书写，包括参数，列表是否都在容器内进行注册，是否返回字符串
        """
        error_index_list = {"return_error": [], "args_error": []}
        error_dep_list = {"return_error": [], "args_error": []}

        for k, v in self._index.items():
            fn_args = v.fn.__annotations__
            _return = fn_args["return"]
            if _return != type(str):
                error_index_list["return_error"].append(k)
            for arg in fn_args:
                if arg not in self._container.factory_fn:
                    error_index_list["args_error"].append(k)

        for k, v in self._depend.items():
            fn_args = v.fn.__annotations__
            _return = fn_args["return"]
            if _return != type(str):
                error_dep_list["return_error"].append(k)
            for arg in fn_args:
                if arg not in self._container.factory_fn:
                    error_dep_list["args_error"].append(k)

        return error_index_list, error_dep_list


index = IndexRunner.add_index
depend = IndexRunner.add_index_depend
factory = Container.factory