import hashlib
def signature(source: str, url: str, title: str="", body: str="") -> str:
    return hashlib.sha1(f"{source}|{url}|{title}|{body[:200]}".encode()).hexdigest()
