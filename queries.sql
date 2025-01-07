-- 1. Total Subscribers per Channel
-- Question: How many subscribers are there for each channel?
SELECT 
    channel_title,
    MAX("cast(channel_info.subscriber_count as bigint)") AS total_subscribers
FROM transform
GROUP BY channel_title
ORDER BY total_subscribers DESC;

-- 2. Total Videos Published per Channel
-- Question: How many videos have been published for each channel?
SELECT 
    channel_title,
    MAX("cast(channel_info.video_count as bigint)") AS total_videos
FROM transform
GROUP BY channel_title
ORDER BY total_videos DESC;

-- 3. Video Publishing Trends Over the Last 12 Months
-- Question: What is the trend for videos published by each channel over the last 12 months?
SELECT 
    channel_title,
    DATE_FORMAT(published_at, '%Y-%m') AS publish_month,
    COUNT(video_id) AS total_videos
FROM youtube_analytics.transform
WHERE published_at >= date_add('year', -1, current_date)
GROUP BY channel_title, DATE_FORMAT(published_at, '%Y-%m')
ORDER BY channel_title, publish_month ASC;

-- 4. Most Viewed Videos (top 5)
-- Question: Which are the most viewed videos?
WITH ranked_videos AS (
    SELECT
        channel_title,
        video_title,
        "cast(video.view_count as bigint)" AS video_views,
        published_at,
        ROW_NUMBER() OVER (PARTITION BY channel_title ORDER BY "cast(video.view_count as bigint)" DESC) AS rank
    FROM youtube_analytics.transform
)
SELECT 
    channel_title,
    video_title,
    video_views,
    published_at
FROM ranked_videos
WHERE rank <= 5
ORDER BY channel_title, rank;



-- 5. Additional Insights
-- Here are some optional queries to derive more insights:

-- Top Channels by Total Views
SELECT 
    channel_title,
    SUM("cast(video.view_count as bigint)") AS total_views
FROM transform
GROUP BY channel_title
ORDER BY total_views DESC;

-- Average Video Engagement (Likes and Comments)
SELECT 
    channel_title,
    AVG("cast(video.like_count as bigint)") AS avg_likes,
    AVG("cast(video.comment_count as bigint)") AS avg_comments
FROM transform
GROUP BY channel_title
ORDER BY avg_likes DESC;
