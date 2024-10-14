import praw
import os
import argparse
import gspread
import csv
import logging

from datetime import datetime
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)

# Constants
SCRAPER_FOLDER_NAME = 'scraper'
REDDIT_FOLDER_NAME = 'reddit'
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive.file",
          "https://www.googleapis.com/auth/drive"]
OAUTH_CREDENTIALS_FILE = 'google_credentials_oauth.json'

# Replace these with your own Reddit app credentials in praw.ini file
reddit = praw.Reddit(
    site_name="Scraper"
)


def get_posts_up_to_date(subreddit_name, min_upvotes, max_posts, storage):
    logging.info(f"Starting to scrape posts from subreddit: {subreddit_name}")

    # Check if the storage target exists and get the most recent post date if available
    last_date_in_file = storage.get_last_post_date(subreddit_name)
    if last_date_in_file:
        logging.info(f"Last post date found in storage: {last_date_in_file}")
    else:
        logging.info("No previous posts found in storage. Retrieving all posts.")

    subreddit = reddit.subreddit(subreddit_name)

    # If an end_date is provided, use it; otherwise, retrieve all posts
    if last_date_in_file:
        end_timestamp = int(datetime.strptime(last_date_in_file, '%Y-%m-%d %H:%M:%S UTC').timestamp())
    else:
        end_timestamp = None  # No date limit if no date is found in the file

    posts = []
    logging.info("Retrieving posts from Reddit...")

    # Use PRAW's `new` generator to paginate through posts
    for post in subreddit.new(limit=None):  # Pagination is handled automatically
        # Check if the post has enough upvotes and if the post's creation date is within the limit
        if post.score >= min_upvotes:
            post_created_time = post.created_utc

            # Stop if the post is older than the end_date (if provided)
            if end_timestamp and post_created_time <= end_timestamp:
                logging.info("Reached posts older than the last saved post. Stopping retrieval.")
                break

            # Add the post to the list
            top_comment_body = ""
            top_comment_score = 0
            if len(post.comments) > 0:
                post.comments.replace_more(limit=0)  # Load all comments
                top_comment = max(post.comments, key=lambda comment: comment.score)
                top_comment_body = top_comment.body
                top_comment_score = top_comment.score

            posts.append({
                "date": datetime.utcfromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
                "title": post.title,
                "post_content": post.selftext,
                "post_vote_count": post.score,
                "top_comment": top_comment_body,
                "comment_vote_count": top_comment_score
            })

            logging.info(f"Retrieved post from date: {posts[-1]['date']}. Retrieved posts: {len(posts)}")

            if len(posts) >= max_posts:
                logging.info(f"Reached the maximum number of posts to retrieve: {max_posts}")
                break

    posts.reverse()

    # Write the data to the selected storage method
    logging.info(f"Writing {len(posts)} posts to storage")
    storage.write_posts(subreddit_name, posts)
    logging.info("Finished writing posts to storage")


class CSVStorage:
    @staticmethod
    def get_last_post_date(subreddit_name):
        """
        Check if the CSV file exists, and if so, return the date of the last entry in the file.
        """
        filename = f"{subreddit_name}.csv"

        # Check if the file exists
        if not os.path.isfile(filename):
            return None  # No file exists, so no date to return

        try:
            # Open the CSV and retrieve the last post date
            with open(filename, mode="r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                rows = list(reader)
                if len(rows) == 0:
                    return None  # File exists but is empty
                return rows[-1]["date"]  # Return the date of the last entry
        except Exception as e:
            logging.error(f"Error reading CSV file: {e}")
            return None

    @staticmethod
    def write_posts(subreddit_name, posts):
        """
        Append posts to a CSV file. Create the file if it doesn't exist.
        """
        filename = f"{subreddit_name}.csv"

        # Define the column headers
        headers = ["date", "title", "post_content", "post_vote_count", "top_comment", "comment_vote_count"]

        # Check if the file exists
        file_exists = os.path.isfile(filename)

        # Open the file in append mode
        with open(filename, mode="a", newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)

            # If the file does not exist or is empty, write the headers first
            if not file_exists or os.stat(filename).st_size == 0:
                writer.writeheader()

            # Write the post data to the CSV file
            for post in posts:
                writer.writerow(post)


class GoogleSheetStorage:
    def __init__(self):
        self._initialize_google_client()
        self.folder_id = self._setup_drive_directory()

    def get_last_post_date(self, subreddit_name):
        """
        Check if the Google Sheet exists in the specified folder, and if so, return the date of the last entry in the sheet.
        """
        query = f"mimeType='application/vnd.google-apps.spreadsheet' and name='{subreddit_name}' and '{self.folder_id}' in parents and trashed=false"
        results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            logging.info(f"Google Sheet for subreddit '{subreddit_name}' not found. Creating a new sheet.")
            # If the sheet does not exist, create it in the specified folder
            file_metadata = {
                'name': subreddit_name,
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'parents': [self.folder_id]
            }
            sheet = self.drive_service.files().create(body=file_metadata, fields='id').execute()
            sheet_id = sheet.get('id')
            sheet = self.client.open_by_key(sheet_id).sheet1
            sheet.append_row(["date", "title", "post_content", "post_vote_count", "top_comment", "comment_vote_count"])
            return None
        else:
            # Open the existing sheet
            sheet_id = items[0]['id']
            sheet = self.client.open_by_key(sheet_id).sheet1
            records = sheet.get_all_records()
            if len(records) == 0:
                return None  # Sheet exists but is empty
            return records[-1]["date"]  # Return the date of the last entry

    def write_posts(self, subreddit_name, posts):
        """
        Append posts to a Google Sheet in the specified folder. Create the sheet if it doesn't exist.
        """
        query = f"mimeType='application/vnd.google-apps.spreadsheet' and name='{subreddit_name}' and '{self.folder_id}' in parents and trashed=false"
        results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            logging.info(f"Google Sheet for subreddit '{subreddit_name}' not found. Creating a new sheet.")
            # Create the sheet in the specified folder
            file_metadata = {
                'name': subreddit_name,
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'parents': [self.folder_id]
            }
            sheet = self.drive_service.files().create(body=file_metadata, fields='id').execute()
            sheet_id = sheet.get('id')
            sheet = self.client.open_by_key(sheet_id).sheet1
            sheet.append_row(["date", "title", "post_content", "post_vote_count", "top_comment", "comment_vote_count"])
        else:
            sheet_id = items[0]['id']
            sheet = self.client.open_by_key(sheet_id).sheet1

        # Write the post data to the Google Sheet
        for post in posts:
            sheet.append_row(
                [post["date"], post["title"], post["post_content"], post["post_vote_count"], post["top_comment"],
                 post["comment_vote_count"]])

    def _initialize_google_client(self):
        """
        Set up OAuth flow to get user credentials and return a Google Sheets client.
        """
        flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CREDENTIALS_FILE, SCOPES)
        self.creds = flow.run_local_server(port=0)
        self.client = gspread.authorize(self.creds)

    def _setup_drive_directory(self):
        """
        Check if the 'scraper' directory exists in Google Drive, and create it if it does not.
        Then, check if the 'reddit' directory exists within 'scraper', and create it if it does not.
        """
        self.drive_service = build('drive', 'v3', credentials=self.creds, cache_discovery=False)
        scraper_folder_id = self._get_or_create_folder(SCRAPER_FOLDER_NAME)
        reddit_folder_id = self._get_or_create_folder(REDDIT_FOLDER_NAME, parent_id=scraper_folder_id)
        return reddit_folder_id

    def _get_or_create_folder(self, folder_name, parent_id=None):
        """
        Check if a folder exists by name and parent, create it if it doesn't exist, and return its ID.
        """
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])

        if items:
            return items[0]['id']
        else:
            # Create folder
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_id:
                file_metadata['parents'] = [parent_id]
            folder = self.drive_service.files().create(body=file_metadata, fields='id').execute()
            return folder.get('id')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Reddit posts and save them to a storage file.")
    parser.add_argument("subreddit_name", type=str, help="The name of the subreddit to scrape.")
    parser.add_argument("--min_upvotes", type=int, default=1,
                        help="The minimum number of upvotes required for a post to be included (default is 1).")
    parser.add_argument("--max_posts", type=int, default=10,
                        help="The maximum number of posts to retrieve (default is 10).")
    parser.add_argument("--storage", type=str, choices=["csv", "gs"], default="csv",
                        help="The storage to store the data: 'csv' or 'gs' for google_sheet (default is 'csv').")
    args = parser.parse_args()

    if args.storage == "csv":
        storage = CSVStorage()
    elif args.storage == "gs":
        storage = GoogleSheetStorage()
    else:
        raise ValueError("Invalid storage option")

    get_posts_up_to_date(subreddit_name=args.subreddit_name, min_upvotes=args.min_upvotes,
                         max_posts=args.max_posts, storage=storage)
