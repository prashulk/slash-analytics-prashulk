import duckdb

def main():    
    conn = duckdb.connect('dev.duckdb')
    
    result = conn.execute("SHOW TABLES").fetchall()
    
    print("- Tables in dev.duckdb:")
    for table in result:
        # Select the head of each table
        head_result = conn.execute(f"SELECT * FROM {table[0]} LIMIT 5").fetchall()
        print(f"\n- Head of {table[0]}:")
        for row in head_result:
            print(f"    - {row}")
        print("\n\n----\n\n")
    conn.close()

if __name__ == "__main__":
    main()
