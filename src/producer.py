import json
import time
import requests
from kafka import KafkaProducer

# --- Kafka Producer Setup ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Your NewsAPI Key ---
API_KEY = "6c795e247392452ea10580e276108eb2" # Replace with your actual key

# --- Keywords to filter by ---
KEYWORDS = ['AI', 'machine learning', 'data', 'technology', 'spark']

def fetch_articles():
    """Fetch real news articles from NewsAPI"""
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": "artificial intelligence OR machine learning OR technology",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 10,        # fetch 10 articles per call
        "apiKey": API_KEY
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        if data.get("status") != "ok":
            print(f"API Error: {data.get('message', 'Unknown error')}")
            return []

        articles = data.get("articles", [])
        print(f"Fetched {len(articles)} articles from NewsAPI")
        return articles

    except requests.exceptions.RequestException as e:
        print(f"Network error: {e}")
        return []

def filter_articles(articles):
    """Filter articles that contain relevant keywords"""
    filtered = []
    for article in articles:
        title = article.get("title", "") or ""
        description = article.get("description", "") or ""
        combined = (title + " " + description).lower()

        if any(keyword.lower() in combined for keyword in KEYWORDS):
            filtered.append({
                "id":          article.get("url", "")[:50],
                "user":        article.get("source", {}).get("name", "Unknown"),
                "title":       title,
                "text":        description[:200] if description else title,
                "publishedAt": article.get("publishedAt", "")
            })
    return filtered

def publish_articles(articles):
    """Publish filtered articles to Kafka topic"""
    for article in articles:
        producer.send('social-posts', article)
        print(f"[PUBLISHED] Source: {article['user']}")
        print(f"            Title:  {article['title']}")
        print(f"            Text:   {article['text'][:80]}...")
        print()





if __name__ == "__main__":
    print("Producer started. Fetching real news from NewsAPI every 30 seconds...")
    print(f"Filtering by keywords: {KEYWORDS}\n")

    while True:
        articles = fetch_articles()
        filtered = filter_articles(articles)

        if filtered:
            publish_articles(filtered)
            producer.flush()
            print(f"--- Batch done: {len(filtered)} article(s) published ---\n")
        else:
            print("--- No matching articles in this batch ---\n")

        print("Waiting 30 seconds before next fetch...\n")
        time.sleep(30)