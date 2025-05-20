from pyspark.sql import functions as F

def get_product_with_categories(products_df, categories_df, product_categories_df):
    # Присоединяем категории к связям
    product_to_category = product_categories_df.join(
        categories_df,
        on='category_id',
        how='left'
    )

    result = products_df.join(
        product_to_category,
        on='product_id',
        how='left'
    ).select(
        'product_name',
        'category_name'
    )

    return result

