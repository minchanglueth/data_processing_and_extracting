from slack_sdk.errors import SlackApiError
from connect_confidential.connect_slack import client_slack

report_crawler = """Hi <@UTEQMCRRS|cal> !
CC: <@UDW03RVGR|cal>
Crawler's status report criteria on {}:
\ta. Total number of crawlingtasks id: {} (ta: 14, ts&nc: 10, s11: >60)
\tb. Status values: {} (should be complete only)
\tc. Failed crawlingtasksid: {}
URLs : {}"""
# nhớ chỉnh tag name nha


class send_message_slack:
    def __init__(self, autotype, count_crlid, valuestats, crlid, report):
        self.autotype = autotype
        self.report = report
        self.count_crawlingtasksid = count_crlid
        self.value_status = valuestats
        self.sql2 = crlid

    def msg_slack(self):
        # topalbum_crawler = message
        report_crawler_updated = report_crawler.format(
            self.autotype,
            self.count_crawlingtasksid,
            self.value_status,
            self.sql2,
            self.report,
        )
        print(report_crawler_updated)

    def send_to_slack(self):
        report_crawler_updated = report_crawler.format(
            self.autotype,
            self.count_crawlingtasksid,
            self.value_status,
            self.sql2,
            self.report,
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
        report_crawler_updated = report_crawler.format(
            self.autotype,
            self.count_crawlingtasksid,
            self.value_status,
            self.sql2,
            self.report,
        )
        try:
            # client_slack.chat_postMessage(
            #     channel="meo-meo-go-go", text=str(report_crawler_updated)
            # )  # MM<3
            client_slack.chat_postMessage(channel='data-auto-report-error', text=str(report_crawler_updated)) #vibbidi-correct
            # client_slack.chat_postMessage(channel='data-auto-error', text=str(report_crawler_updated)) #vibbidi-test
        except SlackApiError as e:
            ## You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
            print(f"Got an error: {e.response['ok']}")
