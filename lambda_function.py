import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
import logging
from googleapiclient.discovery import build
import boto3
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class YouTubeExtractor:
    def __init__(self, api_key):
        self.youtube = build('youtube', 'v3', developerKey=api_key)

    def get_channel_info(self, channel_identifier):
        try:
            # Check if identifier is a channel ID
            if not channel_identifier.startswith('UC'):
                search_response = self.youtube.search().list(
                    q=channel_identifier,
                    type='channel',
                    part='id',
                    maxResults=1
                ).execute()
                if not search_response['items']:
                    return None
                channel_id = search_response['items'][0]['id']['channelId']
            else:
                channel_id = channel_identifier

            channel_response = self.youtube.channels().list(
                part='snippet,statistics,contentDetails',
                id=channel_id
            ).execute()

            if not channel_response['items']:
                return None

            channel_info = channel_response['items'][0]
            return {
                'channel_id': channel_info['id'],
                'title': channel_info['snippet']['title'],
                'description': channel_info['snippet']['description'],
                'published_at': channel_info['snippet']['publishedAt'],
                'view_count': channel_info['statistics']['viewCount'],
                'subscriber_count': channel_info['statistics'].get('subscriberCount', 'N/A'),
                'video_count': channel_info['statistics']['videoCount'],
                'playlist_id': channel_info['contentDetails']['relatedPlaylists']['uploads']
            }
        except Exception as e:
            logger.error(f"Error getting channel info: {e}")
            return None

    def get_channel_videos(self, playlist_id, max_results=50):
        try:
            videos = []
            next_page_token = None

            while True:
                playlist_response = self.youtube.playlistItems().list(
                    part='snippet,contentDetails',
                    playlistId=playlist_id,
                    maxResults=min(50, max_results - len(videos)),
                    pageToken=next_page_token
                ).execute()

                video_ids = [item['contentDetails']['videoId'] for item in playlist_response['items']]
                video_response = self.youtube.videos().list(
                    part='statistics,contentDetails',
                    id=','.join(video_ids)
                ).execute()

                for playlist_item, video_item in zip(playlist_response['items'], video_response['items']):
                    videos.append({
                        'video_id': playlist_item['contentDetails']['videoId'],
                        'title': playlist_item['snippet']['title'],
                        'description': playlist_item['snippet']['description'],
                        'published_at': playlist_item['snippet']['publishedAt'],
                        'view_count': video_item['statistics'].get('viewCount', 0),
                        'like_count': video_item['statistics'].get('likeCount', 0),
                        'comment_count': video_item['statistics'].get('commentCount', 0),
                        'duration': video_item['contentDetails']['duration']
                    })

                next_page_token = playlist_response.get('nextPageToken')
                if not next_page_token or len(videos) >= max_results:
                    break

            return videos
        except Exception as e:
            logger.error(f"Error getting channel videos: {e}")
            return None

def upload_to_s3(data, bucket, key):
    try:
        s3_client = boto3.client('s3')
        s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, ensure_ascii=False))
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
        return True
    except ClientError as e:
        logger.error(f"S3 upload error: {e}")
        return False

def analyze_channel_data(channel_data, videos_data):
    # Get current time and 12 months ago
    current_time = datetime.utcnow()
    twelve_months_ago = current_time - timedelta(days=365)

    analysis = {
        'channel_stats': {
            'name': channel_data['title'],
            'subscribers': channel_data['subscriber_count'],
            'total_videos': channel_data['video_count'],
            'total_views': channel_data['view_count']
        },
        'video_trends': defaultdict(int),
        'top_videos': []
    }

    # Process videos
    videos_by_views = sorted(videos_data, key=lambda x: int(x['view_count']), reverse=True)
    
    # Get top 10 videos
    analysis['top_videos'] = [{
        'title': video['title'],
        'video_id': video['video_id'],
        'view_count': video['view_count'],
        'published_at': video['published_at']
    } for video in videos_by_views[:10]]

    # Count videos per month for the last 12 months
    for video in videos_data:
        published_date = datetime.strptime(video['published_at'], "%Y-%m-%dT%H:%M:%SZ")
        if published_date >= twelve_months_ago:
            month_key = published_date.strftime('%Y-%m')
            analysis['video_trends'][month_key] += 1

    # Convert defaultdict to regular dict for JSON serialization
    analysis['video_trends'] = dict(analysis['video_trends'])
    
    return analysis

def lambda_handler(event, context):
    try:
        API_KEY = os.environ['YOUTUBE_API_KEY']
        S3_BUCKET = os.environ['S3_BUCKET_NAME']
        channels = ['@straitstimesonline', '@TheBusinessTimes', '@Tamil_Murasu', '@zaobaodotsg', '@BeritaHarianSG1957']

        extractor = YouTubeExtractor(API_KEY)
        
        for channel in channels:
            channel_info = extractor.get_channel_info(channel)
            if channel_info:
                videos = extractor.get_channel_videos(channel_info['playlist_id'], max_results=100)
                if videos:
                    analysis = analyze_channel_data(channel_info, videos)
                    
                    # Generate timestamp for the filename
                    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                    key = f"raw/{channel_info['title'].replace(' ', '_')}_{timestamp}.json"
                    
                    # Upload to S3
                    upload_to_s3({
                        'channel_info': channel_info,
                        'videos': videos,
                        'analysis': analysis
                    }, S3_BUCKET, key)

        return {
            'statusCode': 200,
            'body': json.dumps('YouTube data extraction completed successfully')
        }
        
    except Exception as e:
        logger.error(f"Lambda execution error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }