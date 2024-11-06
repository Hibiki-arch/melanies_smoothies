# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col,when_matched
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title(":cup_with_straw: Example Streamlit App :cup_with_straw:")
st.write(
    """
        choose the fruits you want  in your custom Smoothie!
    """
);

# ↓テーブル表示

session = get_active_session()

# my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
# st.dataframe(data=my_dataframe, use_container_width=True)

# editable_df = st.data_editor(my_dataframe)


my_orderframe = session.table("SMOOTHIES.PUBLIC.ORDERS").select(
    col('order_uid'),
    col('INGREDIENTS'),
    col('NAME_ON_ORDER'),
    col('order_filled')
)



st.dataframe(data=my_orderframe, 
             use_container_width=True
            )

# 編集可能なテーブルにする
editordertable_df = st.data_editor(my_orderframe)

time_to_insert = st.button('Submit Order')

if time_to_insert:
    st.success("Someone clicked the button")
    # snow mergeステートメントの作成
    og_dataset = session.table("smoothies.public.orders")
    # st.write(og_dataset);
    edited_dataset = session.create_dataframe(editordertable_df)
    # st.write(edited_dataset);
    
    try:
        og_dataset.merge(edited_dataset
                    , (og_dataset['order_uid'] ==edited_dataset['order_uid'])
                    , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                     );
        st.success("Order Updated")
    except:
        st.write("something went wrong")
else:
    st.success("there are no pending order right")

