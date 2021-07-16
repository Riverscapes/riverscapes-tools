"""" Math module to safely run expressions
"""
# from sympy import zoo, oo, nan
from sympy.parsing.sympy_parser import parse_expr, TokenError


class EquationError(Exception):
    """Raised when the input value is too small"""
    pass


def safe_eval(eval_fn: str, fn_params: dict = None) -> float:
    """Eval is notorious for being hard to debug so we try a safety function to make it all work

    Args:
        eval_fn (str): An equation as a string
        fn_params (dict): A dictionary of parameter names and corresponding values

    Raises:
        Exception: [description]
        e: [description]

    Returns:
        float: [description]
    """
    try:
        # eval seems to mutate the fn_params object so we pass in a copy so that we can report on the errors if needed
        result = parse_expr(eval_fn, local_dict=fn_params)
        if result.is_infinite:
            raise EquationError('Equation produced infinite result: eq: "{}", variables: "{}"'.format(eval_fn, fn_params))
        if not result.is_number:
            raise EquationError('Equation produced non-numeric result: "{}" eq: "{}", variables: "{}"'.format(result, eval_fn, fn_params))
        return float(result)
    except TokenError as err:
        raise EquationError('Error parsing equation: "{}", variables: "{}", Err: {}'.format(eval_fn, fn_params, err)) from None
    except Exception as err:
        raise err
