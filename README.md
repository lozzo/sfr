# SFR 
Simple Flow Runer
一个简单的流程执行框架，包含流程

```python
from sfr import index,factory,depend,IndexRunner

class Mysql:
    def __init__(self) -> None:
        print("mysql")
        
@factory()
def get_mysql()->Mysql:
    return Mysql()

@depend(cache="auto", depend_temp_view=[])
def yl1(ny:Mysql) -> str:
    print('yl1')
    return "1"

@index(index_name="指标1", depend_temp_view=[yl1, "依赖2"])
def zb1() -> str:
    print('zb1')
    return "1"

@index(index_name='指标2', depend_temp_view=[])
def zb2() -> str:
    print('zb2')
    return "1"

@depend(name="依赖2", cache="auto", depend_temp_view=[yl1])
def yl2() -> str:
    print('yl2')
    return "1"

if __name__ == "__main__":
    a = IndexRunner(auto_cache_number=1, debug=True)
    a.run(index_name=["指标1", "指标2"])
```