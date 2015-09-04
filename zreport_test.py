import unittest
import zreport

class ZReportTest(unittest.TestCase):

    def test_main(self):
        zreport.ZReport().run()

if __name__ == "__main__":
    unittest.main()