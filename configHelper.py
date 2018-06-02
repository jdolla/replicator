def getConnStr(parts):
    if 'connStr' in parts and parts['connStr']:
        return parts['connStr']

    port = parts['port'] if 'port' in parts else '1433'

    connStr = f"DRIVER={{{parts['driver']}}};" \
        f"SERVER={parts['host']},{port};" \
        f"DATABASE={parts['database']};"

    if 'trusted' in parts and parts['trusted']:
        connStr += "Trusted_Connection=yes;"

    if 'username' in parts:
        connStr += f"UID={parts['username']};"

    if 'password' in parts:
        connStr += f"PWD={parts['password']};"

    return connStr
