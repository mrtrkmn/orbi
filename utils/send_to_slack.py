# author: mrtrkmn@github
# description: This file contains several functions to send message/file for given slack channel

import argparse
from os import environ
from os import path
from slack_sdk import WebClient
import sys

root_path = path.dirname(path.abspath(__file__))
sys.path.append(root_path)


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
    slack_client.files_upload_v2(
        channels=channel,
        file=file_path,
        initial_comment=message,
    )


def send_message_to_slack(message, channel):
    """
    Send a message to a Slack channel.

    :param message (str): The message to send.
    :param channel (str): The channel to send the message to.

    :return:
    None
    """
    slack_client = WebClient(token=environ.get("SLACK_TOKEN"))
    slack_client.chat_postMessage(
        channel=channel,
        text=message,
    )


if __name__ == "__main__":
    # set arg parser
    parser = argparse.ArgumentParser(description="Send a file to a Slack channel.")
    file_path = parser.add_argument("--file_path", type=str, help="The path to the file to send.")
    slack_channel = parser.add_argument("--slack_channel", type=str, help="The Slack channel to send the file to.")
    message = parser.add_argument("--message", type=str, help="The message to send with the file.")
    args = parser.parse_args()

    send_file_to_slack(args.file_path, args.slack_channel, args.message)
