
from functools import wraps
from typing import TypeVar, Callable, Dict, Optional, Type, TYPE_CHECKING, Any
from sfr.exceptions import AnnotationsException
_T = TypeVar("_T")

if TYPE_CHECKING:
    import builtins

class Container(object):

    factory_fn: Dict[Type[_T], Callable[..., Type[_T]]] = {}
    object_map: Dict[Type[_T], _T] = {}
    
    def __init__(self) -> None:
        pass
    
    @classmethod
    def get(cls, obj_type: Type[_T]) -> _T:
        obj = cls.object_map.get(obj_type)
        if obj is None:
            obj = cls.factory_fn.get(obj_type)()
            cls.object_map.update({obj_type: obj})
        return obj

    @classmethod
    def factory(cls, *, name: Optional[str] = None, options: Any = None):
        """装饰器,用此装饰器装饰的工厂函数会被注册的容器内部
        被装饰的工厂函数必须标明返回的类型

            >>> @Contaier.factory()
                def mysql_factory(mysql_info:str)->Mysql:
                    return Mysql(mysql_info)

        Args:
            name (Optional[str], optional): 定义工厂返回的对象别名,如果不传，则在注入的时候根据函数参数的注解自动注入. Defaults to None.
            options (Any, optional): 初始化的参数. Defaults to None.
        """
        def decorator(fn: Callable[..., Type[_T]]):
            return_type = fn.__annotations__['return']
            if return_type == type(None):
                raise AnnotationsException("工厂函数必须标注返回对象类型")
            cls.factory_fn.update({return_type: fn})

            @wraps
            def inner(fn):
                return fn()

            return inner

        return decorator
