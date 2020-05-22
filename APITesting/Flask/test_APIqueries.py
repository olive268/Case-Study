import unittest

from Flask.APIqueries import app

class ErrorTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    def tearDown(self):
        pass

    def test_pagenotfound_statuscode1(self):
        result = self.app.get("/invalid/url")
        self.assertEqual(result.status_code, 404)


    def test_pagenotfound_statuscode2(self):
        result = self.app.get("/tweets_per_country")
        self.assertNotEqual(result.status_code, 404)

    def test_pagenotfound_statuscode3(self):
        result = self.app.get("/tweets_per_country/<country_name>")
        self.assertNotEqual(result.status_code, 404)


    def test_pagenotfound_statuscode4(self):
        result = self.app.get("/tweets_per_country_date")
        self.assertNotEqual(result.status_code, 404)

    def test_pagenotfound_statuscode5(self):
        result = self.app.get("/tweets_per_country_date/<country_name>")
        self.assertNotEqual(result.status_code, 404)

    def test_pagenotfound_statuscode6(self):
        result = self.app.get("/tweets_per_country_date/date/<date>")
        self.assertNotEqual(result.status_code, 404)

    def test_pagenotfound_statuscode7(self):
        result = self.app.get("/overall_top_100words")
        self.assertNotEqual(result.status_code, 404)

    def test_pagenotfound_statuscode8(self):
        result = self.app.get("/top_100words_country")
        self.assertNotEqual(result.status_code, 404)


    def test_pagenotfound_statuscode9(self):
        result = self.app.get("/top_100words_country/<country_name>")
        self.assertNotEqual(result.status_code, 404)

    def test_pagenotfound_statuscode10(self):
        result = self.app.get("/overall_donations_country")
        self.assertNotEqual(result.status_code, 404)

    def test_pagenotfound_statuscode11(self):
        result = self.app.get("/overall_donations_country/<country_name>")
        self.assertNotEqual(result.status_code, 404)

    def test_pagenotfound_statuscode12(self):
        result = self.app.get("/tweets_per_country_date/date_country/<country_name>/<date>")
        self.assertNotEqual(result.status_code, 404)

