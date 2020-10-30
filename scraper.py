import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from database import Database
from collections import defaultdict 
import hashlib
from utils import split_url
from nltk.util import ngrams

def scraper(url, resp, db, url_cache, fingerprint_cache):
    #TODO: handle responses (2xx, 3xx)

    if resp.status >= 600:
        print("Status " + str(resp.status) + ": " + resp.error)
        return []
    elif resp.status >= 400:
        print("Status " + str(resp.status) + ": " + resp.raw_response)
        return []
    elif resp.status < 200:
        print("Status " + str(resp.status) + ": " + resp.raw_response)
        return []

    # TEXT PARSING
    links, text = extract_next_links(url, resp) # get links + text
    tokens = get_tokens(text)

    # COMPUTE FINGERPRINT 
    fingerprint = create_fingerprint(tokens)
    if similar_page_exists(fingerprint, db, fingerprint_cache):
        return []

    print(len(fingerprint))
    print(len(serialize_prints(fingerprint)))

    # TODO: Clean up links (Remove fragments / Add base url to relative links)
    #print(links)

    # GET FREQUENCIES
    freqs = compute_word_frequencies(tokens)
    remove_stop_words(freqs)
    #print(freqs)
    input("Hit enter when ready: ")

    db.upsert_word_counts(freqs)
    #db.get_word_counts()
    
    # RETURN NEW LINKS
    # Add current url & current fingerprint to sets (this change is only saved locally- will have to actually be added later)
    url_cache.add(url)
    fingerprint_cache.add(tuple(fingerprint))
    links = remove_visited_links(links, db, url_cache)

    #we do this in a batch and not one at a time in should_visit because it is more efficient to let sqlite do logic
    #print the newly filtered list of links
    return ([link for link in links if should_visit(link)], tuple(fingerprint)) #do any more last minute filtering on links one at a time

def similar_page_exists(fingerprint, db, cache):
    pass

def remove_visited_links(links, db, cache):
    print("========url_cache=========")
    print(cache)
    unvisited_links = list(filter(lambda x: x not in cache or not db.url_exists(split_url(x)), links))
    print("========unvisited_links=========")
    print(unvisited_links)
    return unvisited_links
    #return [] #return empty list for now

def serialize_prints(fingerprint):
    line = ""

    for value in fingerprint:
        line += str(value)

    return line


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

    # print(hash_vals)
    # input("Hit enter when ready: ")
    return hash_vals



def dup_check(prints1, prints2):
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
            #print("removing " + word + " from freqs")
            del freqs[word]

def get_tokens(text):
    return re.split("[^a-zA-Z0-9']+", text.lower())

def compute_word_frequencies(tokens):
    #freqs = defaultdict(int)
    freqs = dict()
    for token in tokens:
        if token in freqs:
            freqs[token] += 1
        else:
            freqs[token] = 1

    return freqs

def should_visit(link): 
    return is_valid(link)

def extract_next_links(link, resp):
    urls = set()
    soup = BeautifulSoup(resp.raw_response.text, 'html.parser')
    for url in soup.find_all('a'):
        url = url.get('href')
        if url == None or url == "/" or url[0] == "#":    #will get rid of some fragments
            continue
        else:
            urls.add(urljoin(link, url))   #if url is relative, will create absolute

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
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
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
    
