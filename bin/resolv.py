import requests, json, sqlite3, time
import dns.resolver
from urllib.parse import urlparse, urlsplit
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass, field
from queue import Empty, Queue
from requests_doh import DNSOverHTTPSAdapter, add_dns_provider


@dataclass(frozen=True, kw_only=True)
class CustomDBSAdaptor(requests.adapters.HTTPAdapter):
    resolver: dns.resolver.Resolver

    def resolve(self, hostname):
        answer = [str(a) for a in self.resolver.resolve(hostname).rrset]
        if not answer:
            raise Exception(f"Could not resolve {hostname}")
        return answer[0]

    def send(self, request, **kwargs):

        url_parts = urlsplit(request.url)
        resolved_ip = self.resolve(url_parts.hostname)

        request.url = request.url.replace(
            "https://" + url_parts.hostname,
            "https://" + resolved_ip,
        )

        pool_kwargs = self.poolmanager.connection_pool_kw
        if url_parts.scheme == "https" and resolved_ip:
            pool_kwargs["server_hostname"] = url_parts.hostname  # SNI
            pool_kwargs["assert_hostname"] = url_parts.hostname
        else:
            # theses headers from a previous request may have been left
            pool_kwargs.pop("server_hostname", None)
            pool_kwargs.pop("assert_hostname", None)

        request.headers["Host"] = url_parts.hostname
        return super(CustomDBSAdaptor, self).send(request, **kwargs)


resolver = dns.resolver.Resolver()

resolver.nameservers = ["127.0.0.1"]

answer = [str(a) for a in resolver.resolve("foo.tthtesting.co.uk.").rrset]
print(answer)

# add_dns_provider("google", "https://dns.google/dns-query")
# adapter = DNSOverHTTPSAdapter(provider="google")
# session = requests.Session()
# session.mount("https://", adapter)
# session.mount("http://", adapter)


# @dataclass(frozen=True, kw_only=True)
# class SpiderReponse:
#     url: str
#     status_code: int
#     reason: str
#     headers: dict = field(default_factory=dict)

#     @classmethod
#     def from_response(cls, url: str, res: requests.Response):
#         headers = {k: v for k, v in res.headers.lower_items()}
#         return cls(
#             url=url,
#             status_code=res.status_code,
#             reason=res.reason,
#             headers=headers,
#         )


# def check(url, *, timeout: int = 10):
#     try:
#         res = session.head(url, timeout=timeout)
#         return SpiderReponse.from_response(url, res)
#     except requests.exceptions.Timeout as e:
#         return SpiderReponse(url=url, status_code=1000, reason="timeout")
#     except Exception as e:
#         return SpiderReponse(url=url, status_code=1010, reason=str(e))
