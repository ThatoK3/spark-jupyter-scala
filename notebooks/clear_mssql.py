#!/usr/bin/env python3
"""
Clear MS SQL Server Database
"""
import pymssql

conn = pymssql.connect('healthcare-mssql', 'sa', 'HealthCare2024!', 'healthcare', autocommit=True)
cursor = conn.cursor()

# Drop tables if exist
cursor.execute("IF OBJECT_ID('hospital_administration', 'U') IS NOT NULL DROP TABLE hospital_administration")
cursor.execute("IF OBJECT_ID('healthcare_providers', 'U') IS NOT NULL DROP TABLE healthcare_providers")
print("✅ Tables dropped")

# Verify
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
tables = cursor.fetchall()
print(f"Remaining tables: {[t[0] for t in tables]}")

conn.close()
print("✅ Database cleared")