import logging
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Configure logging to display info-level messages in the console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
token=os.getenv("SLACK_BOT_TOKEN")
print(token)
# Instantiate a Slack client using the bot token stored in the SLACK_BOT_TOKEN environment variable
client = WebClient(token=token)
def post_file_content_to_slack(file_path, channel_id="C07SW82KU0Z"):
    """
    Reads the content of a text file and posts it to a Slack channel.
    
    Parameters:
        file_path (str): Path to the text file to be read.
        channel_id (str, optional): ID of the Slack channel where the message will be posted. 
                                    Defaults to 'C07SW82KU0Z'.
    
    Returns:
        bool: Returns True if the message is successfully posted, otherwise False.
    """
    try:
        # Open and read the content of the text file
        with open(file_path, 'r') as file:
            message_content = file.read()
    except Exception as e:
        # Log an error if there's an issue reading the file
        logger.error(f"Error reading file {file_path}: {e}")
        return False

    if message_content:
        try:
            # Call the chat.postMessage method from the Slack client to post the message
            result = client.chat_postMessage(
                channel=channel_id, 
                text=message_content
            )
            # Log success, showing the message timestamp (ts)
            logger.info(f"Message posted successfully: {result['ts']}")
            return True

        except SlackApiError as e:
            # Log an error if there's an issue with the Slack API call
            logger.error(f"Error posting message: {e.response['error']}")
            return False
    else:
        # Log an error if the file was empty or couldn't be read
        logger.error(f"Failed to read content from {file_path}")
        return False

# Example usage:
# Replace 'your_file_path_here.txt' with the actual path to your text file
#file_path = "files/asian_market_closing_data.txt"
#post_file_content_to_slack(file_path)
