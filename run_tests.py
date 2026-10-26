"""Run the whole anti-regression suite.

Every test here is a guard: a red test means the implementation is wrong, not
that the test should be changed.
"""

import sys
import unittest

ROOT = __file__.rsplit("\\", 1)[0] if "\\" in __file__ else __file__.rsplit("/", 1)[0]
sys.path.insert(0, ROOT)


def suite():
    loader = unittest.TestLoader()
    return loader.discover("tests", top_level_dir=ROOT)


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite())
    sys.exit(0 if result.wasSuccessful() else 1)
