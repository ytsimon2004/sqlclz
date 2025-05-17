import functools
import inspect
import textwrap
from typing import Any, Callable, TypeVar, Union, Type

from typing_extensions import LiteralString

from . import expr

__all__ = ['as_func_expr']


def as_func_expr(f=None, *, func=expr.SqlFuncOper):
    # noinspection PyShadowingNames
    def _as_func_expr(f):
        func_name = f.__name__.upper()
        s = inspect.signature(f)
        para = []
        code = []
        locals = dict(__oper__=func, __func__=f)

        n_none = 0

        for n, p in s.parameters.items():
            if p.kind == inspect.Parameter.VAR_POSITIONAL:
                para.append('*' + n)
                n_none = -1
                break
            elif p.default is inspect.Parameter.empty:
                para.append(n)
            elif p.default is None:
                para.append((n, 'None'))
                n_none += 1
            else:
                raise RuntimeError()

            if p.annotation == 'LiteralString' or p.annotation == LiteralString:
                code.append(f'{n} = __literal__(repr({n}))')
                locals['__literal__'] = expr.SqlLiteral

        if func == expr.SqlFuncOper:
            func_call = '__func__,'
        else:
            func_call = ''

        if n_none == -1:
            args = ', '.join(para)
            code.append(f'return __oper__("{func_name}", {func_call} {args})')
        elif n_none == 0:
            args = ', '.join(para)
            code.append(f'return __oper__("{func_name}", {func_call} {args})')
        elif n_none == 1:
            assert isinstance(para[-1], tuple)
            z = para[-1][0]
            code.append(f'if {z} is None:')
            args = ', '.join(para[:-1])
            code.append(f'  return __oper__("{func_name}", {func_call} {args})')
            code.append('else:')
            args = args + ', ' + z
            code.append(f'  return __oper__("{func_name}", {func_call} {args})')
        else:
            args = ', '.join(para[:-n_none])
            for i in range(n_none, 0, -1):
                assert isinstance(para[-i], tuple)
                z = para[-i][0]
                code.append(f'if {z} is None:')
                code.append(f'  return __oper__("{func_name}", {func_call} {args})')
                args = args + ', ' + z
            code.append(f'return __oper__("{func_name}", {func_call} {args})')

        ret = create_fn(f.__name__, para, '\n'.join(code), locals=locals)
        return functools.wraps(f)(ret)

    if f is None:
        return _as_func_expr
    else:
        return _as_func_expr(f)


T = TypeVar('T')
PARA_TYPE = Union[str, tuple[str, Union[str, Type]], tuple[str, Union[None, str, Type], str]]
PARA_TYPE_LIST = list[PARA_TYPE]
RET_TYPE = Union[None, str, Type]


def _create_fn_para_from_func(func: Callable) -> str:
    para = []

    s = inspect.signature(func)
    p = None
    for arg_name, arg in s.parameters.items():

        if arg.annotation is inspect.Parameter.empty:
            arg_type = ''
        elif isinstance(arg.annotation, str):
            arg_type = f': {arg.annotation}'
        else:
            arg_type = f': {arg.annotation.__name__}'

        if arg.kind != p:
            if p == inspect.Parameter.POSITIONAL_ONLY:
                para.append('/')
                if arg.kind == inspect.Parameter.KEYWORD_ONLY:
                    para.append('*')
            elif p == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                if arg.kind == inspect.Parameter.KEYWORD_ONLY:
                    para.append('*')

        if arg.kind == inspect.Parameter.VAR_POSITIONAL:
            arg_prefix = '*'
        elif arg.kind == inspect.Parameter.VAR_KEYWORD:
            arg_prefix = '**'
        else:
            arg_prefix = ''

        if arg.default is inspect.Parameter.empty:
            arg_default = ''
        else:
            arg_default = f'={repr(arg.default)}'

        para.append(f'{arg_prefix}{arg_name}{arg_type}{arg_default}')

        p = arg.kind

    if s.return_annotation is inspect.Parameter.empty:
        ret_ann = ''
    elif isinstance(s.return_annotation, str):
        ret_ann = f'-> {s.return_annotation}'
    else:
        ret_ann = f'-> {s.return_annotation.__name__}'

    para = ', '.join(para)
    return f'({para}){ret_ann}'


def _create_fn_para_from_list(sign: PARA_TYPE_LIST | tuple[PARA_TYPE_LIST, RET_TYPE]) -> str:
    para = []

    if isinstance(sign, list):
        ret_ann = ''
    else:
        sign, ret_type = sign
        if ret_type is None:
            ret_ann = '-> None'
        elif isinstance(ret_type, str):
            ret_ann = f'-> {ret_type}'
        elif isinstance(ret_type, type):
            ret_ann = f'-> {ret_type.__name__}'
        else:
            raise TypeError()

    for arg in sign:
        if isinstance(arg, str):
            para.append(arg)
        elif len(arg) == 2:
            arg_name, arg_type = arg
            if isinstance(arg_type, str):
                para.append(f'{arg_name}: {arg_type}')
            elif isinstance(arg_type, type):
                para.append(f'{arg_name}: {arg_type.__name__}')
            else:
                raise TypeError()
        elif len(arg) == 3:
            arg_name, arg_type, default = arg
            if arg_type is None:
                para.append(f'{arg_name}={default}')
            elif isinstance(arg_type, str):
                para.append(f'{arg_name}: {arg_type}={default}')
            elif isinstance(arg_type, type):
                para.append(f'{arg_name}: {arg_type.__name__}={default}')
            else:
                raise TypeError()
        else:
            raise TypeError()

    para = ', '.join(para)
    return f'({para}){ret_ann}'


def create_fn(name: str,
              sign: PARA_TYPE_LIST | tuple[PARA_TYPE_LIST, RET_TYPE] | Callable,
              body: str = 'pass',
              *,
              globals=None,
              locals: dict[str, Any] = None) -> Callable[..., T]:
    """

    Example:

    >>> add = create_fn('add', (['a', 'b'], int), 'return a + b')
    >>> add(1, 2)
    3
    >>> def add_sign(a: int, b:int) -> int:
    ...     pass
    >>> add = create_fn('add', add_sign, 'return a + b')
    >>> add(1, 2)
    3

    Signature Example

    >>> def f(a, b:int, c=0, d:int=1) -> int:
    (['a', ('b', int), ('c', None, '0'), ('d', int, '1')], int)

    reference: dataclasses._create_fn

    :param name:
    :param sign: `([arg_name|(arg_name, arg_type)|(arg_name, arg_type?, str(default)),...], ret_type)`
    :param body:
    :param globals:
    :param locals:
    :return:
    """
    locals = locals or {}

    if inspect.isfunction(sign):
        sign = _create_fn_para_from_func(sign)
    else:
        sign = _create_fn_para_from_list(sign)

    func = textwrap.indent(f'def {name}{sign}:\n' + textwrap.indent(body, prefix='  '), '  ')

    local_params = ', '.join(locals.keys())
    code = f'def __neuralib_dynamic_generate_function__({local_params}):\n{func}\n  return {name}'

    # print(code)
    namespace = {}
    exec(code, globals, namespace)
    return namespace['__neuralib_dynamic_generate_function__'](**locals)
