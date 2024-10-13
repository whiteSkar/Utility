import praw
from datetime import datetime
import os
import argparse
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv

# Replace these with your own Reddit app credentials in praw.ini file
reddit = praw.Reddit(
    site_name="Scraper"
)


def get_posts_up_to_date(subreddit_name, min_upvotes, max_posts, storage_method):
    # Check if CSV or Google Sheet exists and get the most recent post date if available
    last_date_in_file = None
    if storage_method == 'csv':
        last_date_in_file = get_last_post_date_csv(subreddit_name)
    elif storage_method == 'google_sheet':
        last_date_in_file = get_last_post_date_google_sheet(subreddit_name)

    subreddit = reddit.subreddit(subreddit_name)

    # If an end_date is provided, use it; otherwise, retrieve all posts
    if last_date_in_file:
        end_timestamp = int(datetime.strptime(last_date_in_file, '%Y-%m-%d %H:%M:%S UTC').timestamp())
    else:
        end_timestamp = None  # No date limit if no date is found in the file

    posts = []

    # Use PRAW's `new` generator to paginate through posts
    for post in subreddit.new(limit=None):  # Pagination is handled automatically
        # Check if the post has enough upvotes and if the post's creation date is within the limit
        if post.score >= min_upvotes:
            post_created_time = post.created_utc

            # Stop if the post is older than the end_date (if provided)
            if end_timestamp and post_created_time <= end_timestamp:
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

            if len(posts) >= max_posts:
                break

    posts.reverse()

    # Write the data to the selected storage method
    if storage_method == 'csv':
        write_to_csv(subreddit_name, posts)
    elif storage_method == 'google_sheet':
        write_to_google_sheet(subreddit_name, posts)


def get_last_post_date_csv(subreddit_name):
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
        print(f"Error reading CSV file: {e}")
        return None


def write_to_csv(subreddit_name, posts):
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


def get_last_post_date_google_sheet(subreddit_name):
    """
    Check if the Google Sheet exists, and if so, return the date of the last entry in the sheet.
    """
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open(subreddit_name).sheet1
        records = sheet.get_all_records()
        if len(records) == 0:
            return None  # Sheet exists but is empty
        return records[-1]["date"]  # Return the date of the last entry
    except Exception as e:
        print(f"Error accessing Google Sheet: {e}")
        return None


def write_to_google_sheet(subreddit_name, posts):
    """
    Append posts to a Google Sheet. Create the sheet if it doesn't exist.
    """
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open(subreddit_name).sheet1
    except gspread.exceptions.SpreadsheetNotFound:
        sheet = client.create(subreddit_name).sheet1
        sheet.append_row(["date", "title", "post_content", "top_comment", "post_vote_count", "comment_vote_count"])

    # Write the post data to the Google Sheet
    for post in posts:
        sheet.append_row([post["date"], post["title"], post["post_content"], post["top_comment"], post["post_vote_count"], post["comment_vote_count"]])


# Example usage with command-line-like input
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Reddit posts and save them to a CSV file.")
    parser.add_argument("subreddit_name", type=str, help="The name of the subreddit to scrape.")
    parser.add_argument("--min_upvotes", type=int, default=10,
                        help="The minimum number of upvotes required for a post to be included (default is 100).")
    parser.add_argument("--max_posts", type=int, default=10,
                        help="The maximum number of posts to retrieve (default is 100).")
    parser.add_argument("--storage_method", type=str, choices=["csv", "google_sheet"], default="csv",
                        help="The method to store the data: 'csv' or 'google_sheet' (default is 'csv').")

    args = parser.parse_args()

    # Retrieve posts up to the most recent date in the file, or all if no file/entries exist
    get_posts_up_to_date(subreddit_name=args.subreddit_name, min_upvotes=args.min_upvotes,
                         max_posts=args.max_posts, storage_method=args.storage_method)
