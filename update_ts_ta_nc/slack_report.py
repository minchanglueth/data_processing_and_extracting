from slack_sdk.errors import SlackApiError
from connect_confidential.connect_slack import client_slack

report_updatestats = """Hi <@UTEQMCRRS|cal> !
CC: <@UDW03RVGR|cal>
Report update's status on {}:
\ta. Invalid Track/AlbumUUID/GenreID/Category : {} 
\tb. Compare url_extract vs url_update: {} 
URLs : {}"""
# nhớ chỉnh tag name nha


class send_message_slack:
    def __init__(self, autotype, list_url, comparison, report_url):
        self.autotype = autotype
        self.list_url = list_url
        self.comparison = comparison
        self.report_url = report_url

    def msg_slack(self):
        # topalbum_crawler = message
        report_crawler_updated = report_updatestats.format(
            self.autotype, self.list_url, self.comparison, self.report_url
        )
        print(report_crawler_updated)

    def send_to_slack(self):
        report_crawler_updated = report_updatestats.format(
            self.autotype, self.list_url, self.comparison, self.report_url
        )
        try:
            # client_slack.chat_postMessage(
            #     channel="minchan-testing", text=str(report_crawler_updated)
            # )  # MM<3
            client_slack.chat_postMessage(channel='unit-collection', text=str(report_crawler_updated)) #vibbidi-correct
            # client_slack.chat_postMessage(channel='data-auto-error', text=str(report_crawler_updated)) #vibbidi-test
        except SlackApiError as e:
            ## You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
            print(f"Got an error: {e.response['ok']}")

    def send_to_slack_error(self):
        report_crawler_updated = report_updatestats.format(
            self.autotype, self.list_url, self.comparison, self.report_url
        )
        try:
            # client_slack.chat_postMessage(
            #     channel="minchan-error", text=str(report_crawler_updated)
            # )  # MM<3
            client_slack.chat_postMessage(channel='data-auto-report-error', text=str(report_crawler_updated)) #vibbidi-correct
            # client_slack.chat_postMessage(channel='data-auto-error', text=str(report_crawler_updated)) #vibbidi-test
        except SlackApiError as e:
            ## You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
            print(f"Got an error: {e.response['ok']}")
