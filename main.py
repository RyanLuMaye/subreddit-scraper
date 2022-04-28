import praw
import json
import os
from os.path import join
from praw.models import MoreComments

COMMENTS_DIRECTORY = "./comments"
SUBMISSIONS_DIRECTORY = "./submissions"


def load_analyzed_submissions(subreddit):
    return load_analyzed_objects(join(SUBMISSIONS_DIRECTORY, subreddit))


def load_analyzed_comments(subreddit):
    return load_analyzed_objects(join(COMMENTS_DIRECTORY, subreddit))


def load_analyzed_objects(path):

    if not os.path.exists(path):
        return set()

    analyzed_comments = set()
    for file in os.listdir(path):
        # Don't include .json
        analyzed_comments.add(file[:-5])

    return analyzed_comments


def get_authentication_data():

    with open("./authentication.json") as file:
        return json.load(file)


def sort_comments(unknown_comments, comments, unloaded_comments):

    for comment in list(unknown_comments):
        if isinstance(comment, MoreComments):
            unloaded_comments.append(comment)
        else:
            comments.append(comment)


def comment_generator(submission, upvote_threshold=10, notification_threshold=5):

    submission.comment_sort = "top"

    comments = []
    unloaded_comments = []
    sort_comments(list(submission.comments), comments, unloaded_comments)

    # Discard unloaded top level comments in the case the min upvoted is already below the threshold we want
    if len(comments) > 0 and comments[-1].ups < upvote_threshold:
        unloaded_comments = []

    comment_expansion_streak = 0

    while len(comments) > 0 or len(unloaded_comments) > 0:

        if len(comments) > 0:
            comment = comments.pop(0)

            # Don't bother searching through children if upvote threshold isn't met.
            if comment.ups >= upvote_threshold:
                sort_comments(list(comment.replies), comments, unloaded_comments)

            comment_expansion_streak = 0
            yield comment, len(comments) + len(unloaded_comments)

        else:
            unloaded_comment = unloaded_comments.pop(0)
            sort_comments(list(unloaded_comment.comments()), comments, unloaded_comments)

            comment_expansion_streak += 1
            if comment_expansion_streak > notification_threshold:
                print(f"\rLoading More comments... ({len(unloaded_comments)} left)                            ", end="")

    return


# See https://praw.readthedocs.io/en/stable/tutorials/comments.html
def analyze_submission_comments(submission, analyzed_comments, save_location, upvote_threshold):

    comments_saved = 0
    comments_repeated = 0
    comments_ignored = 0

    print(f"Loading comments for post: \"{submission.title}\"", end="")

    for comment, queued in comment_generator(submission):

        print(f"\rAnalyzing Post: \"{submission.title}\" (Saved: {comments_saved} Repeat: {comments_repeated} Ignored: {comments_ignored} Queued: {queued})", end="")
        if comment.ups < upvote_threshold:
            comments_ignored += 1
            continue

        if comment.id in analyzed_comments:
            comments_repeated += 1
            continue

        save_comment(comment, save_location)
        analyzed_comments.add(comment.id)
        comments_saved += 1

    print(f"\rFinished Analyzing Post: \"{submission.title}\" (Saved: {comments_saved} Repeat: {comments_repeated} Ignored: {comments_ignored})")


def save_comment(comment, subreddit_directory):

    try:
        json_contents = json.dumps({
            "text": comment.body,
            "author": comment.author_fullname,
            "id": comment.id,
            "parent": comment.parent_id if not comment.is_root else "",
            "upvotes": comment.ups,
            "post": comment.link_id
        })
    except Exception:
        return

    with open(join(subreddit_directory, comment.id + ".json"), "w+") as file:
        file.write(json_contents)


def save_submission(submission, subreddit_directory):

    json_contents = json.dumps({
        "title": submission.title,
        "text": submission.selftext,
        "author": submission.author_fullname,
        "id": submission.id,
        "upvotes": submission.ups,
    })

    with open(join(subreddit_directory, submission.id + ".json"), "w+") as file:
        file.write(json_contents)


def scrape_subreddit(reddit, subreddit, upvote_threshold, submission_search_strategy, skip_saved_submissions):

    analyzed_comments = load_analyzed_comments(subreddit)
    analyzed_submissions = load_analyzed_submissions(subreddit)
    print(f"\nScraping Subreddit: {subreddit}\n")

    for submission in submission_search_strategy(reddit.subreddit(subreddit)):

        if skip_saved_submissions and submission.id in analyzed_submissions:
            print(f"Skipping already analyzed submission \"{submission.title}\"")
            continue

        subreddit_comment_directory = join(COMMENTS_DIRECTORY, subreddit)
        subreddit_submission_directory = join(SUBMISSIONS_DIRECTORY, subreddit)

        os.makedirs(subreddit_comment_directory, exist_ok=True)
        os.makedirs(subreddit_submission_directory, exist_ok=True)

        save_submission(submission, subreddit_submission_directory)
        analyze_submission_comments(submission, analyzed_comments, subreddit_comment_directory, upvote_threshold)


def scrape_subreddits(*subreddits, upvote_threshold=10, submission_search_strategy=lambda x: x.top(limit=50), skip_saved_submissions=False):

    authentication = get_authentication_data()
    reddit = praw.Reddit(
        client_id=authentication["client_id"],
        client_secret=authentication["secret"],
        user_agent=authentication["uuid"]
    )

    for subreddit in subreddits:
        scrape_subreddit(reddit, subreddit, upvote_threshold, submission_search_strategy, skip_saved_submissions)


if __name__ == '__main__':
    scrape_subreddits("wallstreetbets", "worldnews", "gaming", "IdiotsInCars", "nba", skip_saved_submissions=True)
