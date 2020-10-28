import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from database import Database
from collections import defaultdict 
import hashlib

def scraper(url, resp, db, cache):
    #TODO: behavior if bad status? should we just discard the url 
    # or try requesting the url again?
    if resp.status > 599:
        print("Status " + str(resp.status) + ": " + resp.error)
        return []
    elif resp.status > 200:
        print("Status " + str(resp.status) + ": " + resp.raw_response)
        return []	

    # TEXT PARSING
    links, text = extract_next_links(url, resp)
    print(links)
    freqs, tokens = compute_word_frequencies(text)
    #print(freqs)
    #create_fingerprint(tokens)
    #input("Hit enter when ready: ")
    remove_stop_words(freqs)
    print(freqs)
    input("Hit enter when ready: ")

    db.upsert_word_counts(freqs)
    #db.get_word_counts()
    #input("Hit enter when ready: ")

    # RETURN NEW LINKS
    links = remove_visited_links(links, db, cache)
    #we do this in a batch and not one at a time in should_visit because it is more efficient to let sqlite do logic
    #print the newly filtered list of links
    return [link for link in links if should_visit(link)] #do any more last minute filtering on links one at a time

def remove_visited_links(links, db, cache):
    print("========cache=========")
    print(cache)
    unvisited_links = list(filter(lambda x: x not in cache, links))
    visited_links = list(filter(lambda x: x in cache, links))

    print("=======unvisited_links=========")
    print(unvisited_links)
    print("=======visited_links==========")
    print(visited_links)

    db_links = db.get_visited_urls()
    print("===========db links============")
    print(db_links)
    input("Hit enter when ready: ")

    # TODO: ensure proper logic, db_links should be full urls
    unvisited_links = list(filter(lambda x: x not in db_links, unvisited_links))
    #print("unvisited_links")
    #print(unvisited_links)
    #input("Hit enter when ready: ")

    #return unvisited_links
    return []    #return empty list for now

def create_fingerprint(words):
    #TODO: look into nltk, has ngram func
    #https://stackoverflow.com/questions/17531684/n-grams-in-python-four-five-six-grams
    n = 3    # going to create 3-grams
    hash_vals = []

    for i in range(0, len(words) - n):
        gram = words[i] + " " + words[i+1] + " " + words[i+2]
        hex_hash = hashlib.sha256(gram.encode()).hexdigest()
        hex_hash = int(hex_hash, 16)
        if hex_hash % n == 0:
            hash_vals.append(hex_hash)

#    print(hash_vals)
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

def compute_word_frequencies(text):
    #TODO: how do we want to handle contractions, most are stop words
    tokens = re.split("[^a-zA-Z0-9']+", text.lower()) #split on non-alphanumeric chars
	
    #freqs = defaultdict(int)
    freqs = dict()
    for token in tokens:
        if token in freqs:
            freqs[token] += 1
        else:
            freqs[token] = 1

    return freqs, tokens    #return tokens for fingerprinting

def should_visit(link): 
    return is_valid(link)

def extract_next_links(link, resp):
    urls = set()
    soup = BeautifulSoup(resp.raw_response.text, 'html.parser')
    for url in soup.find_all('a'):
        url = url.get('href')
        if url == "/" or url[0] == "#":    #will get rid of some fragments
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
    
