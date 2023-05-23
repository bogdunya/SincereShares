from googleapiclient.discovery import build

import creds

def get_service():
    return build('youtube', 'v3', developerKey=creds.yt_api_key)

def by_videos():
    """
    https://developers.google.com/youtube/v3/docs/videos/list
    :return:
    """
    resp = get_service().videos().list(
        id='mggpY1rJEp8',
        part="snippet",
        maxResults="15",
    ).execute()

    [print(r, "\n\n") for r in resp['items']]

def by_search():
    """
    https://developers.google.com/youtube/v3/docs/search/list
    :return:
    """
    resp = get_service().search().list(
        q="azzraelcode",
        part="snippet",
        type='video',
        order='rating',
        maxResults="15",
    ).execute()

    [print(r, "\n\n") for r in resp['items']]

if __name__ == '__main__':
    print("*** AzzraelCode YouTube Subs! ***")
    # by_search()
    by_videos()