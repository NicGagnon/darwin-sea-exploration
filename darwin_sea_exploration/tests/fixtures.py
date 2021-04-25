# third party imports
import pytest
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, BooleanType, LongType, DateType


@pytest.fixture
def sample_data():
    """ Will return only the necessary fields that are actually accessed """
    data = [
        {"id": "FB16866D-AE4D-416F-8848-122B07DA42F5", "received_at": "2018-01-30 18:13:52.221000",
         "anonymous_id": "0A52CDC6-DDDC-4F7D-AA24-4447F6AF2689", "context_app_version": "1.2.3",
         "context_device_ad_tracking_enabled": True, "context_device_manufacturer": "Apple",
         "context_device_model": "iPhone8,4", "context_device_type": "android", "context_locale": "de-DE",
         "context_network_wifi": True, "context_os_name": "android", "context_timezone": "Europe/Berlin",
         "event": "submission_success", "event_text": "submissionSuccess",
         "original_timestamp": "2018-01-30T19:13:43.383+0100", "sent_at": "2018-01-30 18:13:51.000000",
         "timestamp": "2018-01-30 18:13:43.627000", "user_id": "18946", "context_network_carrier": "o2-de",
         "context_device_token": None, "context_traits_taxfix_language": "en-DE"},
        {"id": "AED96FC7-19F1-46AB-B79F-D412117119BD", "received_at": "2018-02-03 18:28:12.378000",
         "anonymous_id": "8E0302A3-2184-4592-851D-B93C32E410AB", "context_device_manufacturer": "Apple",
         "context_device_model": "iPhone8,4", "context_device_type": "ios", "context_library_name": "analytics-ios",
         "context_library_version": "3.6.7", "context_locale": "de-DE", "context_network_wifi": True,
         "context_os_name": "iOS", "event": "registration_initiated", "event_text": "registrationInitiated",
         "original_timestamp": "2018-02-03T19:28:06.291+0100", "sent_at": "2018-02-03 18:28:12.000000",
         "timestamp": "2018-02-03 18:28:06.561000", "context_network_carrier": "o2-de",
         "context_traits_taxfix_language": "de-DE"}
    ]
    return data


@pytest.fixture
def double_count_data():
    """ Will return only the necessary fields that are actually accessed """
    data = [
        {"id": "FB16866D-AE4D-416F-8848-122B07DA42F5", "received_at": "2018-01-30 18:13:52.221000",
         "anonymous_id": "0A52CDC6-DDDC-4F7D-AA24-4447F6AF2689", "context_app_version": "1.2.3",
         "context_device_ad_tracking_enabled": True, "context_device_manufacturer": "Apple",
         "context_device_model": "iPhone8,4", "context_device_type": "android", "context_locale": "de-DE",
         "context_network_wifi": True, "context_os_name": "android", "context_timezone": "Europe/Berlin",
         "event": "submission_success", "event_text": "submissionSuccess",
         "original_timestamp": "2018-01-30T19:13:43.383+0100", "sent_at": "2018-01-30 18:13:51.000000",
         "timestamp": "2018-01-30 18:13:43.627000", "user_id": "18946", "context_network_carrier": "o2-de",
         "context_device_token": None, "context_traits_taxfix_language": "en-DE"},
        {"id": "AED96FC7-19F1-46AB-B79F-D412117119BD", "received_at": "2018-02-03 18:28:12.378000",
         "anonymous_id": "8E0302A3-2184-4592-851D-B93C32E410AB", "context_device_manufacturer": "Apple",
         "context_device_model": "iPhone8,4", "context_device_type": "ios", "context_library_name": "analytics-ios",
         "context_library_version": "3.6.7", "context_locale": "de-DE", "context_network_wifi": True,
         "context_os_name": "iOS", "event": "submission_success", "event_text": "registrationInitiated",
         "original_timestamp": "2018-02-03T19:28:06.291+0100", "sent_at": "2018-02-03 18:28:12.000000",
         "timestamp": "2018-01-30 18:28:06.561000", "context_network_carrier": "o2-de",
         "context_traits_taxfix_language": "de-DE"}
    ]
    return data


@pytest.fixture
def multi_diff_count():
    """ Will return only the necessary fields that are actually accessed """
    data = [
        {"id": "FB16866D-ASFD-416F-8848-122B07DA42F5", "event": "submission_success", "timestamp": "2018-01-30 18:13:43.627000"},
        {"id": "FB16866D-34YH-416F-8848-122B07DA42F5", "event": "submission_success",
         "timestamp": "2018-01-30 18:13:43.627000"},
        {"id": "FB16866D-1234-416F-8848-122B07DA42F5", "event": "submission_success",
         "timestamp": "2018-01-31 18:13:43.627000"},
        {"id": "FB16866D-RTGBE-416F-8848-122B07DA42F5", "event": "submission_success",
         "timestamp": "2018-01-31 18:13:43.627000"},
        {"id": "FB16866D-234T5HY-416F-8848-122B07DA42F5", "event": "submission_success",
         "timestamp": "2018-01-31 18:13:43.627000"},
        {"id": "FB16866D-21345-416F-8848-122B07DA42F5", "event": "registration_initiated",
         "timestamp": "2018-01-31 18:13:43.627000"},
    ]
    return data


@pytest.fixture
def input_schema():
    return StructType([
        StructField('id', StringType(), True),
        StructField('received_at', StringType(), True),
        StructField('anonymous_id', StringType(), True),
        StructField('context_app_version', StringType(), True),
        StructField('context_device_ad_tracking_enabled', BooleanType(), True),
        StructField('context_device_manufacturer', StringType(), True),
        StructField('context_device_model', StringType(), True),
        StructField('context_device_type', StringType(), True),
        StructField('context_library_name', StringType(), True),
        StructField('context_library_version', StringType(), True),
        StructField('context_locale', StringType(), True),
        StructField('context_network_wifi', BooleanType(), True),
        StructField('context_os_name', StringType(), True),
        StructField('context_timezone', StringType(), True),
        StructField('event', StringType(), True),
        StructField('event_text', StringType(), True),
        StructField('original_timestamp', StringType(), True),
        StructField('sent_at', StringType(), True),
        StructField('timestamp', StringType(), True),
        StructField('user_id', StringType(), True),
        StructField('context_network_carrier', StringType(), True),
        StructField('context_device_token', StringType(), True),
        StructField('context_traits_taxfix_language', StringType(), True)
    ])


@pytest.fixture
def output_schema():
    return StructType([
        StructField('event', StringType(), True),
        StructField('date', DateType(), True),
        StructField('count', LongType(), False)
    ])