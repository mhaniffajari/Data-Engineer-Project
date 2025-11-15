import os
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# === CONFIG ===
REVIEW_HTML_FOLDER = "tokopedia_review_html"
PRODUCT_HTML_FOLDER = "tokopedia_product_html"
REVIEW_OUTPUT_CSV = "tokopedia_review.csv"
PRODUCT_OUTPUT_CSV = "tokopedia_product.csv"

# === Helper: Convert Indonesian relative dates ===
def convert_indonesian_date(text):
    text = text.strip().lower()
    today = datetime.today().date()

    if text == 'hari ini':
        return today
    elif 'hari lalu' in text:
        try:
            n_days = int(text.split()[0])
            return today - timedelta(days=n_days)
        except Exception:
            return pd.NaT
    else:
        try:
            return datetime.strptime(text, "%d %b %Y").date()
        except Exception:
            return pd.NaT

# === Delete helper ===
def safe_delete(file_path):
    try:
        os.remove(file_path)
        print(f"Deleted {file_path}")
    except Exception as e:
        print(f"Failed to delete {file_path}: {e}")

# === PARSE REVIEWS ===
def parse_reviews():
    all_data = []
    if not os.path.exists(REVIEW_HTML_FOLDER):
        print(f"Folder '{REVIEW_HTML_FOLDER}' not found. Skipping review parsing.")
        return pd.DataFrame()

    html_files = sorted([f for f in os.listdir(REVIEW_HTML_FOLDER) if f.endswith(".html")])
    print(f"Found {len(html_files)} review HTML files.")

    for filename in html_files:
        filepath = os.path.join(REVIEW_HTML_FOLDER, filename)
        try:
            print(f"Parsing {filename} ...")

            with open(filepath, "r", encoding="utf-8") as f:
                soup = BeautifulSoup(f, "html.parser")

            review_blocks = soup.select('div[data-testid="review-item"]')
            if not review_blocks:
                review_blocks = soup.find_all("article") or soup.find_all(
                    "div", class_=lambda x: x and "review" in x.lower()
                )

            for review in review_blocks:
                try:
                    name_tag = review.select_one('span[data-testid="lblNamaPembeli"], span.name')
                    name = name_tag.get_text(strip=True) if name_tag else ""
                except Exception:
                    name = ""

                try:
                    rating_elements = review.select('svg[fill="#FFC400"], svg[fill*="#FFD45F"]')
                    rating = len(rating_elements) if rating_elements else None
                except Exception:
                    rating = None

                try:
                    variant_tag = review.select_one(
                        'p[data-unify="Typography"].css-19mbq85-unf-heading.e1qvo2ff8'
                    )
                    variant = variant_tag.get_text(strip=True).replace("Varian:", "").strip() if variant_tag else ""
                except Exception:
                    variant = ""

                try:
                    product_tag = review.select_one("p.css-akhxpb-unf-heading.e1qvo2ff8")
                    product_name = product_tag.get_text(strip=True) if product_tag else ""
                except Exception:
                    product_name = ""

                try:
                    date_tag = review.select_one(
                        'p[data-unify="Typography"].css-1rpz5os-unf-heading.e1qvo2ff8'
                    )
                    review_date = date_tag.get_text(strip=True) if date_tag else ""
                except Exception:
                    review_date = ""

                try:
                    text_tag = review.select_one('span[data-testid="lblItemUlasan"]')
                    text = text_tag.get_text(strip=True) if text_tag else ""
                except Exception:
                    text = ""

                all_data.append({
                    "file_name": filename,
                    "product_name": product_name,
                    "reviewer": name,
                    "review_date": review_date,
                    "rating": rating,
                    "variant": variant,
                    "review": text
                })

            # Delete after success parse
            safe_delete(filepath)

        except Exception as e:
            print(f"❌ Error parsing {filename}: {e}")

    df = pd.DataFrame(all_data)
    if not df.empty:
        df = df[df['product_name'] != '']
        df['review_date'] = df['review_date'].apply(convert_indonesian_date)
    return df

# === PARSE PRODUCTS ===
def parse_products():
    all_products = []
    if not os.path.exists(PRODUCT_HTML_FOLDER):
        print(f"Folder '{PRODUCT_HTML_FOLDER}' not found. Skipping product parsing.")
        return pd.DataFrame()

    html_files = sorted([f for f in os.listdir(PRODUCT_HTML_FOLDER) if f.endswith(".html")])
    print(f"Found {len(html_files)} product HTML files.")

    for filename in html_files:
        filepath = os.path.join(PRODUCT_HTML_FOLDER, filename)
        try:
            print(f"Parsing {filename} ...")

            with open(filepath, "r", encoding="utf-8") as f:
                soup = BeautifulSoup(f, "html.parser")

            products = soup.find_all("span", class_="+tnoqZhn89+NHUA43BpiJg==")
            for product in products:
                try:
                    product_name = product.get_text(strip=True)
                    discount_div = product.find_next(
                        "div", class_="urMOIDHH7I0Iy1Dv2oFaNw== HJhoi0tEIlowsgSNDNWVXg=="
                    )
                    discount_price = discount_div.get_text(strip=True) if discount_div else ""
                    original_div = product.find_next("div", class_="e48Kml5BRW9dq8Mopwgv7w==")
                    original_price = original_div.get_text(strip=True) if original_div else ""
                    rating_span = product.find_next("span", class_="_2NfJxPu4JC-55aCJ8bEsyw==")
                    rating = rating_span.get_text(strip=True) if rating_span else ""
                    qty_span = product.find_next("span", class_="u6SfjDD2WiBlNW7zHmzRhQ==")
                    quantity = qty_span.get_text(strip=True) if qty_span else ""

                    all_products.append({
                        "file_name": filename,
                        "product_name": product_name,
                        "discount_price": discount_price,
                        "original_price": original_price,
                        "rating": rating,
                        "quantity": quantity
                    })
                except Exception as e:
                    print(f"⚠️ Skipping a product due to error: {e}")

            # Delete after success parse
            safe_delete(filepath)

        except Exception as e:
            print(f"Error parsing {filename}: {e}")

    df = pd.DataFrame(all_products)

    # === Add category mapping ===
    if not df.empty:
        df['category'] = np.where(df['product_name'].str.lower().str.contains('shampo', case=False, na=False), 'Shampoo', 
                          np.where(df['product_name'].str.lower().str.contains('sabun', case=False, na=False), 'Soap',
                                   np.where(df['product_name'].str.lower().str.contains('body lotion', case=False, na=False), 'Body Lotion',
                                            np.where(df['product_name'].str.lower().str.contains('calming rub cream', case=False, na=False), 'Calming Rub Cream',
                                                     np.where(df['product_name'].str.lower().str.contains('diaper', case=False, na=False), 'Diaper Cream',
                                                              np.where(df['product_name'].str.lower().str.contains('face cream', case=False, na=False), 'Face Cream',
                                                                       np.where(df['product_name'].str.lower().str.contains('sun', case=False, na=False), 'Sunscreen',
                                                                                np.where(df['product_name'].str.lower().str.contains('hair', case=False, na=False), 'Hair Lotion',
                                                                                         np.where(df['product_name'].str.lower().str.contains('essential', case=False, na=False), 'essential oil',
                                                                                                  'Other')))))))))

    return df


# === MAIN ===
if __name__ == "__main__":
    print("Starting Tokopedia HTML → CSV conversion...\n")

    try:
        review_df = parse_reviews()
        if not review_df.empty:
            review_df.to_csv(REVIEW_OUTPUT_CSV, index=False)
            print(f"Saved reviews to {REVIEW_OUTPUT_CSV} ({len(review_df)} rows)")
        else:
            print("No review data found.")
    except Exception as e:
        print(f"Error exporting reviews: {e}")

    try:
        product_df = parse_products()
        if not product_df.empty:
            product_df.to_csv(PRODUCT_OUTPUT_CSV, index=False)
            print(f"Saved products to {PRODUCT_OUTPUT_CSV} ({len(product_df)} rows)")
        else:
            print("No product data found.")
    except Exception as e:
        print(f"Error exporting products: {e}")

    print("\n Done! Conversion complete and HTML files cleaned up.")
