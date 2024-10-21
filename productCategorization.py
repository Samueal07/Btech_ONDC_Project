import streamlit as st
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ondc_products']
collection = db['products']

def get_all_categories():
    return sorted(collection.distinct('category'))

def get_products_by_category(category):
    return list(collection.find({'category': category}))

def update_product_category(product_id, new_category):
    result = collection.update_one(
        {'_id': ObjectId(product_id)},
        {'$set': {'category': new_category}}
    )
    return result.modified_count > 0

def add_new_category(new_category):
    existing_categories = get_all_categories()
    if new_category not in existing_categories:
        # We don't need to explicitly add the category to a separate collection
        # It will be added when a product uses this category
        return True
    return False

def main():
    st.title("Product Categorization for ONDC-like Platform")

    # Sidebar for category management
    st.sidebar.header("Category Management")
    new_category = st.sidebar.text_input("Add New Category")
    if st.sidebar.button("Add Category"):
        if add_new_category(new_category):
            st.sidebar.success(f"Category '{new_category}' added successfully!")
        else:
            st.sidebar.error(f"Category '{new_category}' already exists.")

    # Main content for product categorization
    categories = get_all_categories()
    selected_category = st.selectbox("Select a category to view products", categories)

    if selected_category:
        products = get_products_by_category(selected_category)
        st.write(f"Products in category '{selected_category}':")
        for product in products:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"{product['product_name']} - ₹{product['price']}")
            with col2:
                new_category = st.selectbox(f"Recategorize {product['product_name']}", categories, key=str(product['_id']))
                if new_category != selected_category:
                    if st.button("Update", key=f"update_{product['_id']}"):
                        if update_product_category(product['_id'], new_category):
                            st.success(f"Updated category for {product['product_name']} to {new_category}")
                        else:
                            st.error("Failed to update category")

    # Option to view all products
    if st.button("View All Products"):
        all_products = list(collection.find({}, {'_id': 0}))  # Exclude MongoDB's _id field
        st.json(all_products)

if __name__ == "__main__":
    main()