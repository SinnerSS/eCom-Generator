import pandas as pd
from typing import List, Dict, Any
import pickle
import os

class ProductStore:
    def __init__(self, input_file: str, cache_file: str = "product_cache.pkl"):
        self.input_file = input_file
        self.cache_file = cache_file
        self.products: List[Dict[str, Any]] = []

    def load_products(self) -> None:
        """Load products from CSV and cache them"""
        print("Loading product data...")

        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'rb') as f:
                    self.products = pickle.load(f)
                print(f"Loaded {len(self.products)} products from cache")
                return
            except Exception as e:
                print(f"Cache load failed: {e}")

        products_df = pd.read_csv(self.input_file)
        self.products = products_df[
            ["product_id", "category_id", "category_code", "brand", "price"]
        ].to_dict('records')
        
        # Save to cache
        with open(self.cache_file, 'wb') as f:
            pickle.dump(self.products, f)
        
        print(f"Loaded {len(self.products)} products from CSV")

    def get_products(self) -> List[Dict[str, Any]]:
        """Return the loaded products"""
        if not self.products:
            self.load_products()
        return self.products

    def get_product_count(self) -> int:
        """Return the number of products"""
        return len(self.products)

if __name__ == "__main__":
    store = ProductStore("2019-Nov.csv")
    store.load_products()
    print(f"Total products: {store.get_product_count()}")