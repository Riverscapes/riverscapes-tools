""" Testing for the vector ops

"""
import unittest
from time import sleep
from rscommons import TimerBuckets


class UtilTest(unittest.TestCase):
    """[summary]

    Args:
        unittest ([type]): [description]
    """

    def test_timer_buckets(self):
        """[summary]
        """
        meta1 = {'something': 2, 'somethingElse': "DUMB"}
        meta2 = {'something': 22, 'somethingElse': "DUMBER"}
        my_buckets = TimerBuckets(meta=meta1)

        with TimerBuckets('key1'):
            sleep(2)

        with TimerBuckets('key1'):
            sleep(2)

        with TimerBuckets('key2'):
            sleep(3)

        self.assertAlmostEqual(my_buckets.timers['key1'], 4, 1)
        self.assertAlmostEqual(my_buckets.timers['key2'], 3, 1)
        self.assertAlmostEqual(my_buckets.total, 7, 1)
        self.assertDictEqual(my_buckets.meta, meta1)
        self.assertEqual(len(my_buckets.timers.keys()), 2)

        my_buckets.tick(meta2)
        self.assertDictEqual(my_buckets.meta, meta2)
        self.assertDictEqual(my_buckets.ticks[0].meta, meta1)
        self.assertAlmostEqual(my_buckets.ticks[0].timers['key1'], 4, 1)
        self.assertAlmostEqual(my_buckets.ticks[0].timers['key2'], 3, 1)
        self.assertAlmostEqual(my_buckets.ticks[0].total, 7, 1)
        self.assertEqual(len(my_buckets.ticks), 1)
        self.assertEqual(len(my_buckets.timers.keys()), 0)
        self.assertAlmostEqual(my_buckets.total, 7, 1)

        my_buckets.reset()
        self.assertEqual(len(my_buckets.ticks), 0)
        self.assertEqual(len(my_buckets.timers.keys()), 0)
        self.assertAlmostEqual(my_buckets.total, 0, 1)
