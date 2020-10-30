import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from database import Database
import hashlib
from utils import split_url, get_urlhash
from nltk.util import ngrams
import requests

def scraper(url, resp, db, url_cache, fingerprint_cache):
    #TODO: handle responses (2xx, 3xx)
    if resp.status >= 600:
        print("Status", resp.status, ":", resp.error)
        url_cache[url] = 0
        return []
    elif resp.status >= 400:
        print("Status", resp.status, ":", resp.raw_response)
        url_cache[url] = 0
        # 408 Request Timeout = you can try again 
        # at a later time with the same request headers
        if resp.status == 408:
            return [url]

        # all other 4xx codes, you cannot resend 
        # request without modifying request headers,
        # which we don't control
        return []
    elif resp.status >= 300:    #redirection
        url_hash = get_urlhash(url)
        url_cache[url] = 0

        redirect_loc = resp.raw_response.headers['location']
        if not redirect_loc:    #this shouldn't happen
            return []

        print("Redirect to", redirect_loc)
        return [urljoin(url, redirect_loc)]

    elif resp.status < 200:
        print("Status", resp.status, ":", resp.raw_response)
        url_cache[url] = 0
        return []

    if resp.raw_response.headers['Content-Type'].find("text") == -1: # content type of document isn't text
        return []

    # TEXT PARSING
    links, text = extract_next_links(url, resp) # get links + text
    tokens = get_tokens(text)

    # COMPUTE FINGERPRINT 
    fingerprint = create_fingerprint(tokens)
    url_hash = get_urlhash(url)
    if similar_page_exists(fingerprint, fingerprint_cache):
        return []
    fingerprint_cache[url_hash] = [fingerprint, 0]

    # print(links)

    # GET FREQUENCIES
    freqs = compute_word_frequencies(tokens)
    remove_stop_words(freqs)
    # print(freqs)

    db.upsert_word_counts(freqs)
    
    # RETURN NEW LINKS
    url_cache[url] = 0
    test = [link for link in links if should_visit(link, db, url_cache)]
    print(test)
    # input("Hit enter when ready: ")
    # return [link for link in links if should_visit(link, db, url_cache)]
    return test

def similar_page_exists(fingerprint, cache):
    for url_hash, value in cache.items():
        print_list, rank = value
        if dup_check(fingerprint, print_list):
            fingerprint[url_hash][1] += 1
            return True

    return False

def is_visited(link, db, cache):
    if link in cache:
        cache[link] += 1
    elif not db.url_exists(split_url(link)):
        unvisited_links.append(link)    # where is unvisited_links declared? am i blind

def create_fingerprint(words):
    n = 3    # going to create 3-grams
    hash_vals = []

    n_grams = ngrams(words, n)

    for gram in n_grams:
        gram = " ".join(gram)
        hex_hash = hashlib.sha256(gram.encode()).hexdigest()
        hex_hash = int(hex_hash, 16)
        if hex_hash % (n + 10) == 0:
            hash_vals.append(hex_hash)

    return hash_vals

def dup_check(prints1, prints2):
    if (len(prints1) + len(prints2)) == 0:
        return False

    threshold = 0.79

    intersection = set(prints1).intersection(prints2)
    similarity = len(intersection)/(len(prints1) + len(prints2))

    return similarity > threshold

def remove_stop_words(freqs):
    stop_words = [
        "a", "about", "above", "after", "again", "against", 
        "all", "am", "an", "and", "any", "are", "aren't", "as", "at", 
        "be", "because", "been", "before", "being", "below", "between", 
        "both", "but", "by", "can't", "cannot", "could", "couldn't", 
        "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", 
        "each", "few", "for", "from", "further", "had", "hadn't", "has", 
        "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", 
        "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", 
        "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", 
        "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", 
        "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", 
        "ought", "our", "ours", "ourselves", "out", "over", "own", 
        "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", 
        "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", 
        "them", "themselves", "then", "there", "there's", "these", "they", "they'd", 
        "they'll", "they're", "they've", "this", "those", "through", "to", "too", 
        "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", 
        "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", 
        "where", "where's", "which", "while", "who", "who's", "whom", "why", 
        "why's", "with", "won't", "would", "wouldn't", 
        "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]

    for word in stop_words:
        if word in freqs:
            del freqs[word]

def get_tokens(text):
    return re.split("[^a-zA-Z0-9']+", text.lower())

def compute_word_frequencies(tokens):
    freqs = dict()
    for token in tokens:
        if token in freqs:
            freqs[token] += 1
        else:
            freqs[token] = 1

    return freqs 

def should_visit(link, db, url_cache): 
    return is_valid(link) and not is_visited(link, db, url_cache)

def extract_next_links(link, resp):
    urls = set()
    soup = BeautifulSoup(resp.raw_response.text, 'html.parser')
    for url in soup.find_all('a'):
        url = url.get('href')
        if not url  or url == "/" or url[0] == "#":    #will get rid of some fragments
            continue
        else:
            url = urldefrag(url)[0] # remove the rest of the fragments
            urls.add(urljoin(link, url))   #if url is relative, will create absolute
            # https://docs.python.org/3/library/urllib.parse.html?highlight=urllib%20urljoin#urllib.parse.urljoin
            # reference for above just in case because huh??? what???

    text = soup.get_text(" ", strip = True)
    
    return list(urls), text 

def is_valid(url):
    try:
        if not url: return False
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
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|txt"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv" 
            + r"|r|c|cpp|java|python|m"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

        return (domain_match and not type_match) 
     

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def has_visited(url, visited_urls):
    pass
    
