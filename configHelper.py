def getConnStr(parts):
    if 'connStr' in parts and parts['connStr']:
        return parts['connStr']

    connStr = "DRIVER={{{driver}}};" \
        "SERVER={server}{port};" \
        "DATABASE={database};"

    if 'trusted' in parts and parts['trusted']:
        connStr += "Trusted_Connection=yes;"

    return connStr.format(
        driver=parts['driver']
    )
