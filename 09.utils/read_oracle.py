def read_oracle(query, CONNECTION_INFO):
    proc_start = time.time()              # proc start time
    
    # Connect to Oracle DB (On-premise)
    with cx_Oracle.connect(CONNECTION_INFO) as conn:
        # Execute query and get parsed data from DB
        df = pd.read_sql(query, conn)
        # Disconnect the connection
    
    proc_end = time.time()
    
    # etc.info
    nrows = len(df)
    memory_usage = round(sum(df.memory_usage(deep=True))/1024**2,4)
    BT = "MB" if memory_usage < 1000 else "GB"
    memory_usage = memory_usage if memory_usage < 1000 else round(memory_usage/1024,2)
    elps_time = round(proc_end - proc_start, 4)
    
    print(f'Rows: {nrows}, Size: {memory_usage:,}{BT}, Elapsed time: {elps_time:}.sec')
    
    return(df)