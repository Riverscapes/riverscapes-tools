""" Testing for the vector ops

"""
import unittest
from rscommons.math import safe_eval, EquationError


class MathTest(unittest.TestCase):
    """[summary]

    Args:
        unittest ([type]): [description]
    """

    def test_safe_eval(self):
        """[summary]
        """
        self.assertEqual(safe_eval("1+1"), 2)
        self.assertEqual(safe_eval("(1+1) * 2"), 4)
        self.assertEqual(safe_eval("(1+a) * 2", {"a": 4}), 10)

        self.assertAlmostEqual(safe_eval("0.177 * (a ** 0.397) * (p ** 0.453)", {"a": 1, "p": 1}), 0.177, 5)

        # Now test some problems

        # Divisiuon by zero
        with self.assertRaises(EquationError) as ctx:
            safe_eval("1+1 /0")
        self.assertTrue("Equation produced infinite result" in ctx.exception.args[0])

        # bad equation
        with self.assertRaises(EquationError) as ctx:
            safe_eval("1+((1 /0")
        self.assertTrue("Error parsing equation" in ctx.exception.args[0])

        # Missing varaible
        with self.assertRaises(EquationError) as ctx:
            safe_eval("0.177 * (a ** 0.397) * (p ** 0.453)", {"a": 1})
        self.assertTrue("Equation produced non-numeric result" in ctx.exception.args[0])
