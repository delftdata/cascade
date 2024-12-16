from typing import Any, Generic, Protocol, Type, TypeVar
from cascade.dataflow.dataflow import DataFlow, InvokeMethod

T = TypeVar('T')


class MethodCall(Generic[T], Protocol):
    """A helper class for type-safety of method signature for compiled methods.

    It corresponds to functions with the following signature:
    ```py
    def my_compiled_method(*args: Any, state: T, key_stack: list[str], **kwargs: Any) -> Any:
        ...
    ```

    `T` corresponds to a Python class, which, if modified, should return as the 2nd item in the tuple.
    
    The first item in the returned tuple corresponds to the actual return value of the function.

    The third item in the tuple corresponds to the `key_stack` which should be updated accordingly. 
    Notably, a terminal function should pop a key off the `key_stack`, whereas a function that calls
    other functions should push the correct key(s) onto the `key_stack`.
    """

    def __call__(self, variable_map: dict[str, Any], state: T, key_stack: list[str]) -> dict[str, Any]: ...
    """@private"""


class StatefulOperator(Generic[T]):
    """An abstraction for a user-defined python class. 
    
    A StatefulOperator handles incoming events, such as 
    `cascade.dataflow.dataflow.InitClass` and `cascade.dataflow.dataflow.InvokeMethod`.
    It is created using a class `cls` and a collection of `methods`. 
    
    These methods map a method identifier (str) to a python function. 
    Importantly, these functions are "stateless" in the sense that they are not
    methods, instead reading and modifying the underlying class `T` through a 
    state variable, see `handle_invoke_method`.
    """
    def __init__(self, cls: Type[T], methods: dict[str,  MethodCall[T]], dataflows: dict[str, DataFlow]):
        """Create the StatefulOperator from a class and its compiled methods.
        
        Typically, a class could be comprised of split and non-split methods. Take the following example:

        ```py
        class User:
            def __init__(self, key: str, balance: int):
                self.key = key
                self.balance = balance
            
            def get_balance(self) -> int:
                return self.balance
                
            def buy_item(self, item: Item) -> bool:
                self.balance -= item.get_price()
                return self.balance >= 0
        ```

        Here, the class could be turned into a StatefulOperator as follows:

        ```py
        def user_get_balance(variable_map: dict[str, Any], state: User, key_stack: list[str]):
            key_stack.pop()
            return state.balance
        
        def user_buy_item_0(variable_map: dict[str, Any], state: User, key_stack: list[str]):
            key_stack.append(variable_map['item_key'])
            
        def user_buy_item_1(variable_map: dict[str, Any], state: User, key_stack: list[str]):
            state.balance -= variable_map['item_get_price']
            return state.balance >= 0

        buy_item_dataflow = Dataflow("user.buy_item")
        buy_item_dataflow.add_edge(...)
        
        op = StatefulOperator(
                User, 
                {
                    "buy_item": user_buy_item_0, 
                    "get_balance": user_get_balance, 
                    "buy_item_1": user_buy_item_1
                },
                {
                    "buy_item": buy_item_dataflow
                }
            )

        ```
        """
        # methods maps function name to a function. Ideally this is done once in the object 
        self._methods = methods
        self._cls = cls
        self.dataflows = dataflows
        """A mapping from method names to DataFlows"""
       

    def handle_init_class(self, *args, **kwargs) -> T:
        """Create an instance of the underlying class. Equivalent to `T.__init__(*args, **kwargs)`."""
        return self._cls(*args, **kwargs)

    def handle_invoke_method(self, method: InvokeMethod, variable_map: dict[str, Any], state: T, key_stack: list[str]) -> dict[str, Any]:
        """Invoke the method of the underlying class.
        
        The `cascade.dataflow.dataflow.InvokeMethod` object must contain a method identifier 
        that exists on the underlying compiled class functions. 

        The state `T` and key_stack is passed along to the function, and may be modified. 
        """
        return self._methods[method.method_name](variable_map=variable_map, state=state, key_stack=key_stack)
    