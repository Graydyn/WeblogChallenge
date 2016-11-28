import weblog_stats
import unittest
import pandas as pd

class SplitterTestCase(unittest.TestCase):
    def setUp(self):
        sample_line = '2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2'
        self.split_line = weblog_stats.custom_split(sample_line)

    def test_split_all_fields_present(self):
        self.assertTrue(len(self.split_line) == 13)

    def test_split_last_field_is_url(self):
        self.assertTrue(type(self.split_line[12]) == str)


class SessionizeTestCase(unittest.TestCase):
    def setUp(self):
        sample_rows = '2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2'
        sample_rows = [weblog_stats.custom_split(sample_rows)]
        columns = ['timestamp', 'elb', 'client_port', 'backend_port','request_processing_time','backend_processing_time','response_processing_time','elb_status_code','backend_status_code','received_bytes','sent_bytes','request_type','request_url']
        self.df = pd.DataFrame(sample_rows, columns=columns)

    def test_sessionize_adds_a_field_named_session_id(self):
        df = weblog_stats.sessionize(self.df, 15)
        self.assertTrue('session_id' in df)

class SessionDurationTestCase(unittest.TestCase):
    def setUp(self):
        sample_rows = '2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2'
        sample_rows = [weblog_stats.custom_split(sample_rows)]
        columns = ['timestamp', 'elb', 'client_port', 'backend_port','request_processing_time','backend_processing_time','response_processing_time','elb_status_code','backend_status_code','received_bytes','sent_bytes','request_type','request_url']
        self.df = pd.DataFrame(sample_rows, columns=columns)
        self.df = weblog_stats.sessionize(self.df, 15)
        
    def test_average_time_is_zero_for_a_single_line(self):
        average_duration = weblog_stats.printAverageSessionDurations(self.df)
        self.assertEqual(average_duration.seconds, 0) #should always be zero when run on just a single line of the log

class UniqueUrlsTestCase(unittest.TestCase):
    def setUp(self):
        sample_rows = '2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2'
        sample_rows = [weblog_stats.custom_split(sample_rows)]
        columns = ['timestamp', 'elb', 'client_port', 'backend_port','request_processing_time','backend_processing_time','response_processing_time','elb_status_code','backend_status_code','received_bytes','sent_bytes','request_type','request_url']
        self.df = pd.DataFrame(sample_rows, columns=columns)
        self.df = weblog_stats.sessionize(self.df, 15)
    def test_average_urls_is_one_for_a_single_line(self):
        df = weblog_stats.countUniqueUrlsPerSession(self.df)
        self.assertEqual(df['session_urls'][0], 1)

class MaxSessionTestCase(unittest.TestCase):
    def setUp(self):
        sample_rows = '2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2'
        sample_rows = [weblog_stats.custom_split(sample_rows)]
        columns = ['timestamp', 'elb', 'client_port', 'backend_port','request_processing_time','backend_processing_time','response_processing_time','elb_status_code','backend_status_code','received_bytes','sent_bytes','request_type','request_url']
        self.df = pd.DataFrame(sample_rows, columns=columns)
        self.df = weblog_stats.sessionize(self.df, 15)

    def test_max_session_ip_matches_passed_in_value(self):
        ip = weblog_stats.findMaxSessionTimes(self.df,100000)
        self.assertEqual(ip, '123.242.248.130')

if __name__ == '__main__':
    unittest.main()