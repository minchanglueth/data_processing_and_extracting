from slack_sdk.errors import SlackApiError
from connect_confidential.connect_slack import client_slack

report_updatestats = """Hi <@U01E6SP58HK|cal> !
CC: <@UDW03RVGR|cal>
User info as below:
+ id / username / email: {}
+ {}"""
# nhớ chỉnh tag name nha


class send_message_slack:
    def __init__(self, userinfo, status):
        self.userinfo = userinfo
        self.status = status

    def msg_slack(self):
        # topalbum_crawler = message
        report_crawler_updated = report_updatestats.format(self.userinfo, self.status)
        print(report_crawler_updated)

    def send_to_slack(self):
        report_crawler_updated = report_updatestats.format(self.userinfo, self.status)
        try:
            # client_slack.chat_postMessage(channel='minchan-testing', text=str(report_crawler_updated)) #MM<3
            # client_slack.chat_postMessage(channel='unit-collection', text=str(report_crawler_updated)) #vibbidi-correct
            client_slack.chat_postMessage(
                channel="data-auto-report-error", text=str(report_crawler_updated)
            )  # vibbidi-test
            # client_slack.chat_postMessage(channel='unit-customersupport', text=str(report_crawler_updated)) #vibbidi-correct
        except SlackApiError as e:
            ## You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
            print(f"Got an error: {e.response['ok']}")
