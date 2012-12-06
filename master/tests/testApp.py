#! /usr/bin/env python
import lsst.qserv.master as qMaster
import unittest


class AppTest(unittest.TestCase):
    """Tests... This is a catch-all for driving the query 
    parse/generate/manipulate code.
    """
    def setUp(self):
        global _options
        pass

    def tearDown(self):
        pass

    def testBuild(self):
        pass
        self.assertEqual(1,1)

def main():
    global _options
    unittest.main()


if __name__ == "__main__":
    main()
