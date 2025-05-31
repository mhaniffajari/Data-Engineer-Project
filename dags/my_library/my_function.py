import re
from bs4 import BeautifulSoup
def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)
def count_words(text):
    words = text.split()
    return len(words)

def clean_comments(text):
    # Using BeautifulSoup to remove HTML tags
    soup = BeautifulSoup(text, 'html.parser')
    clean_text = soup.get_text()  # Get text without HTML tags
    # Replace &nbsp; with a space
    clean_text = clean_text.replace('&nbsp;', ' ')
    return clean_text

# Iterating through the DataFrame
def clean_comments_in_row(comments):
    if len(comments) > 0:
        cleaned_comments = [clean_comments(comment['content']) for comment in comments]
    else:
        cleaned_comments = ""
    return cleaned_comments

def clean_sponsor_name(sponsor):
    if sponsor is None or not sponsor.get('name'):
        return ''
    return sponsor.get('name')

def enforce_types(d):
    keys_to_keep = {"id","total", "per_page", "current_page", "comments", "value"}
    for key in keys_to_keep:
        if key in d and d[key] is not None:
            if key in {"id","total", "per_page", "current_page", "value"}:
                d[key] = int(d[key])  # Convert to int if not None
    return d