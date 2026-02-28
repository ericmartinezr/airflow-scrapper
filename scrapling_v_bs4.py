import httpx
import timeit
from bs4 import BeautifulSoup
from scrapling import Fetcher, Selector

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5000.63 Safari/537.36',
    'Accept': 'text/html,application/json,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1'
}

URL = "https://www.emol.com/noticias/Tecnologia/2025/06/13/1169247/extincion-mastodontes-flora-chilena-peligro.html"

with httpx.Client(headers=HEADERS, http2=True, timeout=30.0) as client:
    request = client.get(URL, follow_redirects=True)
    request.raise_for_status()
    bs = BeautifulSoup(request.text, "html5lib")

    # page = Fetcher.get(URL)
page = Selector(request.text, url=URL)


def bs4():
    titulo_noticia = bs.select(
        "h1#cuDetalle_cuTitular_tituloNoticia")[0].get_text()
    bajada_noticia = bs.select(
        "h2#cuDetalle_cuTitular_bajadaNoticia")[0].get_text()
    texto_noticia = "\n".join([
        tag.get_text(strip=True)
        for tag in bs.select(
            "div#cuDetalle_cuTexto_textoNoticia > div"
        )
        if tag.get_text(strip=True)
    ])

    return {
        "titulo": titulo_noticia[:20],
        "bajada": bajada_noticia[:20],
        "noticia": texto_noticia[:20]
    }


def scrapling():
    titulo_noticia = page.css(
        "h1#cuDetalle_cuTitular_tituloNoticia")[0].text
    bajada_noticia = page.css(
        "h2#cuDetalle_cuTitular_bajadaNoticia")[0].text
    texto_noticia = "\n".join([
        tag.get_all_text(strip=True)
        for tag in page.css(
            "div#cuDetalle_cuTexto_textoNoticia > div"
        )
        if tag.get_all_text(strip=True)
    ])

    return {
        "titulo": titulo_noticia[:20],
        "bajada": bajada_noticia[:20],
        "noticia": texto_noticia[:20]
    }


iterations = 500
bs4_total = timeit.timeit(bs4, number=iterations)
scrapling_total = timeit.timeit(scrapling, number=iterations)
print(f"--- Results (Total time for {iterations} iterations) ---")
print(f"BeautifulSoup: {bs4_total:.4f}s")
print(f"Scrapling:     {scrapling_total:.4f}s")
print(f"Speedup:       {bs4_total / scrapling_total:.2f}x faster")
