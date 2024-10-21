import streamlit as st
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ondc_products']

# Define the schema for products
product_schema = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["product_name", "description", "price", "quantity", "categories"],
        "properties": {
            "product_name": {
                "bsonType": "string",
                "description": "must be a string and is required"
            },
            "description": {
                "bsonType": "string",
                "description": "must be a string and is required"
            },
            "price": {
                "bsonType": "number",
                "minimum": 0,
                "description": "must be a non-negative number and is required"
            },
            "quantity": {
                "bsonType": "int",
                "minimum": 0,
                "description": "must be a non-negative integer and is required"
            },
            "categories": {
                "bsonType": "array",
                "description": "must be an array of strings and is required",
                "items": {
                    "bsonType": "string"
                }
            }
        }
    }
}

# Create or update the collections with schema validation
try:
    db.create_collection("products", validator=product_schema)
except CollectionInvalid:
    db.command("collMod", "products", validator=product_schema)

categories_schema = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["name"],
        "properties": {
            "name": {
                "bsonType": "string",
                "description": "must be a string and is required"
            }
        }
    }
}

try:
    db.create_collection("categories", validator=categories_schema)
except CollectionInvalid:
    db.command("collMod", "categories", validator=categories_schema)

products_collection = db['products']
categories_collection = db['categories']

# CSV validation
def validate_csv(df):
    required_columns = ['product_name', 'description', 'price', 'quantity', 'categories']
    return all(col in df.columns for col in required_columns)

# Save products to MongoDB
def save_products(products):
    result = products_collection.insert_many(products)
    return len(result.inserted_ids)

# Process CSV file
def process_csv(file):
    df = pd.read_csv(file)
    if validate_csv(df):
        products = df.to_dict('records')
        for product in products:
            product['price'] = float(product['price'])
            product['quantity'] = int(product['quantity'])
            product['categories'] = [cat.strip() for cat in product['categories'].split(',')]
        return products
    return None

# Get all categories
def get_all_categories():
    return [cat['name'] for cat in categories_collection.find()]

# Add a new category
def add_category(category_name):
    if not categories_collection.find_one({"name": category_name}):
        categories_collection.insert_one({"name": category_name})

# Search products by attribute (name, description, price, quantity, categories)
def search_products(query):
    search_query = {
        "$or": [
            {"product_name": {"$regex": query, "$options": "i"}},
            {"description": {"$regex": query, "$options": "i"}},
            {"category": {"$in": [query]}}  # Adjusting this to work with array-based fields
        ]
    }
    return list(products_collection.find(search_query))


# Main Streamlit application
def main():
    st.title("Bulk Product Upload and Categorization for ONDC-like Platform")
    
    # Sidebar menu options
    menu = ["Upload Products", "Manage Categories", "View Products", "Search Products"]
    choice = st.sidebar.selectbox("Menu", menu)
    
    if choice == "Upload Products":
        uploaded_files = st.file_uploader("Choose CSV files", type="csv", accept_multiple_files=True)

        if uploaded_files:
            all_products = []
            invalid_files = []

            for uploaded_file in uploaded_files:
                products = process_csv(uploaded_file)
                if products:
                    all_products.extend(products)
                    st.success(f"File '{uploaded_file.name}' successfully processed.")
                else:
                    invalid_files.append(uploaded_file.name)
                    st.error(f"Invalid CSV format in file '{uploaded_file.name}'. Skipping this file.")

            if invalid_files:
                st.warning(f"The following files were skipped due to invalid format: {', '.join(invalid_files)}")

            if all_products:
                st.write(f"Total products ready to upload: {len(all_products)}")
                if st.button("Upload All Products"):
                    try:
                        for product in all_products:
                            for category in product['categories']:
                                add_category(category)
                        uploaded_count = save_products(all_products)
                        st.success(f"{uploaded_count} products have been uploaded successfully to MongoDB!")
                    except Exception as e:
                        st.error(f"An error occurred while uploading products: {str(e)}")
            else:
                st.warning("No valid products to upload. Please check your CSV files and try again.")
        
        st.markdown("### CSV Format")
        st.markdown("Your CSV files should have the following columns:")
        st.markdown("- product_name (string)\n- description (string)\n- price (number)\n- quantity (integer)\n- categories (comma-separated string)")

    elif choice == "Manage Categories":
        st.subheader("Manage Categories")
        new_category = st.text_input("Add a new category")
        if st.button("Add Category"):
            if new_category:
                add_category(new_category)
                st.success(f"Category '{new_category}' added successfully!")
            else:
                st.warning("Please enter a category name.")

        st.subheader("Existing Categories")
        categories = get_all_categories()
        for category in categories:
            st.write(category)

    elif choice == "View Products":
        st.subheader("View Products")
        categories = get_all_categories()
        selected_category = st.multiselect("Filter by category", categories)

        query = {"categories": {"$in": selected_category}} if selected_category else {}
        products = list(products_collection.find(query, {'_id': 0}))

        for product in products:
            st.json(product)
    
    elif choice == "Search Products":
        st.subheader("Search Products by Attributes")
        search_term = st.text_input("Enter a search term (product name, description, category):")

        if search_term:
            search_results = search_products(search_term)
            if search_results:
                st.write(f"Search results for '{search_term}':")
                for product in search_results:
                    st.json(product)
            else:
                st.warning(f"No products found for '{search_term}'")

if __name__ == "__main__":
    main()
