import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from database import Database
from collections import defaultdict 

def compute_word_frequencies(text):
    tokens = re.split("[^a-zA-Z0-9]+", text.lower()) #split on non-alphanumeric chars

    freqs = defaultdict(int)
    for token in tokens:
        freqs[token] += 1

    return freqs

def scraper(url, resp):
    if not is_valid(url):
        return []

    db = Database('test.db')
    db.connect()
    db.create_db()
    db.clear_db()

    links, text = extract_next_links(url, resp)
    print(links)
    freqs = compute_word_frequencies(text)
    print(freqs)
    input("Hit enter when ready: ")

    db.insert_word_counts(freqs)
    db.get_word_counts()
    input("Hit enter when ready: ")
    db.close_connection()
    return [link for link in links if should_visit(link)]

def should_visit(link):
    pass

def extract_next_links(url, resp):
    # Implementation requred.
    # print(resp.raw_response.text)
    urls = set()
    soup = BeautifulSoup(resp.raw_response.text, 'html.parser')
    for url in soup.find_all('a'):
        urls.add(url.get('href'))

    text = soup.get_text(" ", strip = True)
    
    return list(urls), text

def is_valid(url):
    try:
        # Append '//' to beginning of url in order for urlparse to detect netloc
        split_url = url.split('//')
        if len(split_url) > 2:
            return False 
        elif len(split_url) == 1:
            url = '//' + url

        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        domain_match = re.match(
            r"(today\.uci\.edu\/department\/information_computer_sciences\/?).*"
            + r"|.*(\.ics\.uci\.edu\/?).*|.*(\.cs\.uci\.edu\/?).*"
            + r"|.*(\.informatics\.uci\.edu\/?).*|.*(\.stat\.uci\.edu\/?).*", url.lower())
        type_match = re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

        return (domain_match and not type_match) 
     

    except TypeError:
        print ("TypeError for ", parsed)
        raise
