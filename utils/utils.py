import argparse
from os import environ

from slack_sdk import WebClient


def send_file_to_slack(file_path, channel, message):
    """
    Send a file to a Slack channel.

    :param file_path (str): The path to the file to send.
    :param channel (str): The channel to send the file to.
    :param message (str): The message to send with the file.

    :return:
    None
    """
    slack_client = WebClient(token=environ.get("SLACK_TOKEN"))
    slack_client.files_upload(
        channels=channel,
        file=file_path,
        initial_comment=message,
    )


if __name__ == "__main__":
    # set arg parser
    parser = argparse.ArgumentParser(description="Send a file to a Slack channel.")
    file_path = parser.add_argument("--file_path", type=str, help="The path to the file to send.")
    slack_channel = parser.add_argument("--slack_channel", type=str, help="The Slack channel to send the file to.")
    message = parser.add_argument("--message", type=str, help="The message to send with the file.")
    args = parser.parse_args()

    send_file_to_slack(args.file_path, args.slack_channel, args.message)
