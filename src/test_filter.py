from producer import filter_articles

# Test articles — some should pass, some should fail
test_articles = [
    {"url": "1", "title": "AI is transforming healthcare", "description": "Machine learning models are being used...", "source": {"name": "BBC"}, "publishedAt": "2026-04-24"},
    {"url": "2", "title": "Best pizza recipes", "description": "How to make the perfect pizza at home", "source": {"name": "Food Network"}, "publishedAt": "2026-04-24"},
    {"url": "3", "title": "New data center opens in Egypt", "description": "Technology infrastructure expanding", "source": {"name": "Reuters"}, "publishedAt": "2026-04-24"},
    {"url": "4", "title": "Football match results", "description": "Team A beat Team B 3-0 last night", "source": {"name": "ESPN"}, "publishedAt": "2026-04-24"},
    {"url": "5", "title": "Spark 4.0 released", "description": "Apache Spark announces major new version", "source": {"name": "TechCrunch"}, "publishedAt": "2026-04-24"},
]

filtered = filter_articles(test_articles)

print("=== FILTER TEST ===")
print(f"Input articles:    {len(test_articles)}")
print(f"Filtered articles: {len(filtered)}")
print(f"Rejected articles: {len(test_articles) - len(filtered)}")
print()

print("--- Articles that PASSED the filter ---")
for a in filtered:
    print(f"  PASS: {a['title']}")

print()
rejected = [a for a in test_articles if a['url'][:50] not in [f['id'] for f in filtered]]
print("--- Articles that were REJECTED ---")
for a in rejected:
    print(f"  REJECT: {a['title']}")

print()
# Assertions
assert len(filtered) == 3, f"Expected 3 filtered articles, got {len(filtered)}"
assert any("AI" in a['title'] for a in filtered), "AI article should pass"
assert any("data" in a['title'].lower() for a in filtered), "Data article should pass"
assert any("Spark" in a['title'] for a in filtered), "Spark article should pass"
assert not any("pizza" in a['title'].lower() for a in filtered), "Pizza article should be rejected"
assert not any("Football" in a['title'] for a in filtered), "Football article should be rejected"

print("=== ALL FILTER TESTS PASSED ✓ ===")