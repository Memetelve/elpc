from __future__ import annotations

from el_price_checker.parse import extract_price


def test_parse_xkom_like_html_extracts_price_pln() -> None:
    html = """
    <html><head>
      <title>GPU - x-kom</title>
      <script type="application/ld+json">
        {
          "@context":"https://schema.org",
          "@type":"Product",
          "name":"Test GPU",
          "offers": {"@type":"Offer","price":"2899.00","priceCurrency":"PLN"}
        }
      </script>
    </head><body></body></html>
    """
    parsed = extract_price(html)
    assert parsed.error is None
    assert parsed.currency == "PLN"
    assert parsed.price_cents == 289900


def test_parse_morele_like_html_extracts_price_pln() -> None:
    html = """
    <html><head>
      <meta property="og:title" content="Karta graficzna XYZ" />
    </head><body>
      <div>cena: 5 033,09 zł</div>
    </body></html>
    """
    parsed = extract_price(html)
    assert parsed.error is None
    assert parsed.currency == "PLN"
    assert parsed.price_cents == 503309


def test_parse_amazon_like_html_extracts_price() -> None:
    html = """
    <html><head>
      <title>Amazon product</title>
      <script type="application/ld+json">
        {
          "@context":"https://schema.org",
          "@type":"Product",
          "name":"Test",
          "offers": {"@type":"Offer","price":"199.99","priceCurrency":"EUR"}
        }
      </script>
    </head><body></body></html>
    """
    parsed = extract_price(html)
    assert parsed.error is None
    assert parsed.currency == "EUR"
    assert parsed.price_cents == 19999
