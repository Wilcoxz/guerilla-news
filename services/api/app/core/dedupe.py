import hashlib

def signature(source: str, url: str, title: str="", body_first200: str="") -> str:
    base = f"{source}|{url}|{title}|{(body_first200 or '')[:200]}"
    return hashlib.sha1(base.encode()).hexdigest()
