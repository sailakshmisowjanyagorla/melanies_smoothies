# Import Python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("""Orders that need to filled.""")

# Get the current Snowflake session
cnx = st.connection("snowflake")
session = cnx.session()

# Get the pending orders from the Snowflake table
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == 0)

# If there are any pending orders, show them in an editable table
if my_dataframe:
    editable_df = st.data_editor(my_dataframe)
    submitted = st.button('Submit')

    if submitted:
        # Original table in Snowflake
        og_dataset = session.table("smoothies.public.orders")
        # Create a dataframe from the edited table
        edited_dataset = session.create_dataframe(editable_df)

        try:
            # Merge changes into the original Snowflake table
            og_dataset.merge(
                edited_dataset,
                (og_dataset['order_uid'] == edited_dataset['order_uid']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )

            st.success("Order(s) Updated!", icon='👍')
        except Exception as e:
            st.write('Something went wrong:', e)

# If no pending orders
else:
    st.success('👍 There are no pending orders right now', icon='👍')
