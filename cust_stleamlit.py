# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
from snowflake.snowpark.context import get_active_session

cnx = st.connection("snowflake)
session = cnx.session()

# Write directly to the app
st.title(":cup_with_straw: 注文フォーム :cup_with_straw:")
st.write(
    """
        choose the fruits you want  in your custom Smoothie!
    """
)

# ↓テーブル表示

my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
st.dataframe(data=my_dataframe, use_container_width=True)


# ↓ selector
option = st.selectbox(
    
    "What is the most your favorite fruit?",
    my_dataframe,
)
st.write("Your favorite fruite is:", option)

# 名前入力欄
name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your smoothie will be", name_on_order)



# ↓テーブル内のカテゴリの複数選択
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    my_dataframe,
    max_selection=5
)

# ↓表示のラインを整える＝if ingredients_list is not null : then do everything below this line is indented
if ingredients_list:
    # ↓選択項目をリスト表示
    st.write(ingredients_list)
    
    # ↓選択項目をテキスト表示
    st.text(ingredients_list)

    
    # リストを文字列に変換
    # 変数設定
    ingredients_string = '';
    
    # for文
for fruit_chosen in ingredients_list:
    ingredients_string += fruit_chosen + '　'
    st.write(ingredients_string)


# # 選択アイテム/名前のテーブル格納
my_insert_stmt = """ insert into smoothies.public.orders(INGREDIENTS,NAME_ON_ORDER)
    values (
        '""" + ingredients_string + """',
        '""" +name_on_order+ """'
        )"""

st.write(my_insert_stmt)

# ボタン配置
time_to_insert = st.button('反映')
if time_to_insert:
    session.sql(my_insert_stmt).collect()
    st.success('注文したよん!', icon="✅")
