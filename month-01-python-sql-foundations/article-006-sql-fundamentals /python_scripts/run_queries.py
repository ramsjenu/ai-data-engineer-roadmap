# run_queries.py - Execute SQL queries from Python

import psycopg2
import pandas as pd
from tabulate import tabulate
from colorama import Fore, Style, init
import os

init(autoreset=True)


def execute_query(query, description="Query"):
    """Execute SQL query and display results"""
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="ecommerce_db",
            user="admin",
            password="root"
        )
        
        print(f"{Fore.CYAN}{'='*70}")
        print(f"📊 {description}")
        print(f"{'='*70}{Style.RESET_ALL}\n")
        
        # Execute query
        df = pd.read_sql(query, conn)
        
        # Display results
        if len(df) > 0:
            print(tabulate(df.head(20), headers='keys', tablefmt='psql', showindex=False))
            print(f"\n{Fore.GREEN}✅ Returned {len(df)} rows{Style.RESET_ALL}\n")
        else:
            print(f"{Fore.YELLOW}⚠️  No results returned{Style.RESET_ALL}\n")
        
        conn.close()
        return df
        
    except Exception as e:
        print(f"{Fore.RED}❌ Error: {e}{Style.RESET_ALL}\n")
        return None


if __name__ == "__main__":
    # Example: Run CTEs query
    query = """
    WITH customer_spend AS (
        SELECT customer_id, SUM(total_amount) AS total_spent
        FROM orders
        WHERE order_status='Delivered'
        GROUP BY customer_id
        ),
    ranked AS (
        SELECT customer_id, total_spent, 
            RANK() OVER (ORDER BY total_spent DESC) AS rank
        FROM customer_spend
    )
    SELECT c.first_name, c.last_name, r.total_spent, r.rank
    FROM ranked r
    JOIN customers c ON r.customer_id = c.customer_id
    WHERE r.rank <= 10;
    """
    
    execute_query(query, "RANKING HIGH SPENDING CUSTOMERS")