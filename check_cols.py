import pyodbc
from app.config import get_db_config

c = get_db_config()
cs = (
    "DRIVER={" + c.driver + "};"
    "SERVER=" + c.server + ";"
    "DATABASE=" + c.database + ";"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)
cn = pyodbc.connect(cs)
cur = cn.cursor()
cur.execute("""
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='schema_migrations'
ORDER BY ORDINAL_POSITION;
""")
print([r[0] for r in cur.fetchall()])
cn.close()
