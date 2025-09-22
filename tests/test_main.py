import pytest
from db_help.mysql import MySQLManager




# @pytest.fixture
def test_connect_db():
    DB_HOST = "127.0.0.1"
    DB_USER = "root"
    DB_PASSWORD = "1234" # 替换为你的 MySQL root 密码
    DB_NAME = "my_app_db"
    # db_manager_no_db = MySQLManager(DB_HOST, DB_USER, DB_PASSWORD)
    # if db_manager_no_db.create_database(DB_NAME):
    #     print(f"数据库 '{DB_NAME}' 准备就绪。")
    db_manager = MySQLManager(DB_HOST, DB_USER, DB_PASSWORD, database=DB_NAME)

    return db_manager


def test_create_table(db_manager):
    table_name = "users"
    columns_def = "id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE NOT NULL, age INT"
    db_manager.create_table(table_name, columns_def)

def test_create_database_():
    conn = connect_to_db()
    create_database_and_table


def test_insert_user(db_manager):
    table_name = "users"
    db_manager = test_connect_db()
    user1_id = db_manager.insert(table_name, {'name': 'Alice', 'email': 'alice@example.com', 'age': 30})
    user2_id = db_manager.insert(table_name, {'name': 'Bob', 'email': 'bob@example.com', 'age': 25})
    user3_id = db_manager.insert(table_name, {'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35})
    db_manager.insert(table_name, {'name': 'David', 'email': 'david@example.com', 'age': 40}) # 额外插入一个


def test_mysql():

    # 数据库连接配置 (请根据你的实际情况修改)
    DB_HOST = "127.0.0.1"
    DB_USER = "root"
    DB_PASSWORD = "1234" # 替换为你的 MySQL root 密码
    DB_NAME = "my_app_db"

    # 1. 初始化管理器 (不指定数据库，用于创建数据库)
    db_manager_no_db = MySQLManager(DB_HOST, DB_USER, DB_PASSWORD)

    # 创建数据库
    if db_manager_no_db.create_database(DB_NAME):
        print(f"数据库 '{DB_NAME}' 准备就绪。")
        # 2. 现在连接到我们新创建或已存在的数据库
        db_manager = MySQLManager(DB_HOST, DB_USER, DB_PASSWORD, database=DB_NAME)

        # 创建表
        table_name = "users"
        columns_def = "id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE NOT NULL, age INT"
        if db_manager.create_table(table_name, columns_def):
            # --- 插入数据 ---
            print("\n--- 插入数据 ---")
            user1_id = db_manager.insert(table_name, {'name': 'Alice', 'email': 'alice@example.com', 'age': 30})
            user2_id = db_manager.insert(table_name, {'name': 'Bob', 'email': 'bob@example.com', 'age': 25})
            user3_id = db_manager.insert(table_name, {'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35})
            db_manager.insert(table_name, {'name': 'David', 'email': 'david@example.com', 'age': 40}) # 额外插入一个

            # 插入重复邮件（会报错）
            db_manager.insert(table_name, {'name': 'Alice Clone', 'email': 'alice@example.com', 'age': 31})

            # 批量插入
            print("\n--- 批量插入数据 ---")
            bulk_data = [
                ('Eve', 'eve@example.com', 28),
                ('Frank', 'frank@example.com', 45)
            ]
            db_manager.bulk_insert(table_name, ['name', 'email', 'age'], bulk_data)

            # --- 查询数据 ---
            print("\n--- 查询所有用户 ---")
            all_users = db_manager.select(table_name, fetch_all=True)
            if all_users:
                for user in all_users:
                    print(user)

            print("\n--- 查询年龄大于30的用户 ---")
            older_users = db_manager.select(table_name, conditions="age > %s", params=(30,), order_by="age DESC")
            if older_users:
                for user in older_users:
                    print(user)

            print("\n--- 查询 ID 为 1 的用户 ---")
            user_by_id_1 = db_manager.select(table_name, conditions="id = %s", params=(1,), fetch_all=False)
            if user_by_id_1:
                print(user_by_id_1)

            # --- 更新数据 ---
            if user1_id: # 假设 user1_id 成功插入
                print(f"\n--- 更新 ID 为 {user1_id} 的用户 ---")
                db_manager.update(table_name, {'age': 31, 'name': 'Alice Smith'}, conditions="id = %s", params=(user1_id,))

            # 再次查询更新后的用户
            print(f"\n--- 再次查询 ID 为 {user1_id} 的用户 ---")
            updated_user = db_manager.select(table_name, conditions="id = %s", params=(user1_id,), fetch_all=False)
            if updated_user:
                print(updated_user)


            # --- 删除数据 ---
            if user2_id: # 假设 user2_id 成功插入
                print(f"\n--- 删除 ID 为 {user2_id} 的用户 ---")
                db_manager.delete(table_name, conditions="id = %s", params=(user2_id,))

            # 验证删除
            print(f"\n--- 验证删除：查询 ID 为 {user2_id} 的用户 ---")
            deleted_user_check = db_manager.select(table_name, conditions="id = %s", params=(user2_id,), fetch_all=False)
            if deleted_user_check is None:
                print(f"用户 ID {user2_id} 已成功删除。")
            else:
                print(f"用户 ID {user2_id} 仍在数据库中: {deleted_user_check}")

            # --- 最终查询所有用户 ---
            print("\n--- 最终所有用户列表 ---")
            final_users = db_manager.select(table_name, fetch_all=True)
            if final_users:
                for user in final_users:
                    print(user)
            else:
                print("没有找到任何用户。")

        # 关闭数据库连接
        db_manager.close()
    db_manager_no_db.close() # 关闭用于创建数据库的连接