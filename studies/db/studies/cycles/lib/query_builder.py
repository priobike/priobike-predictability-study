def get_relevant_observations_for_given_timeranges(datastream_ids: list, timeranges: list, limit: int):
    """
    Returns a query that returns the observations for the given datastream IDs and timeranges.
    The timeranges are given as a list of tuples, where each tuple contains the start and end time of the timerange.
    """
    
    if len(timeranges) == 0:
        raise ValueError("No timeranges given.")
    
    ids_string = "("
    for datastream_id in datastream_ids:
        ids_string = ids_string + str(datastream_id) + ","
    ids_string = ids_string[:-1]
    ids_string = ids_string + ")"
    
    timeranges_string = "("
    for i in range(len(timeranges)):
        if i > 0:
            timeranges_string = timeranges_string + "OR"
        timeranges_string = timeranges_string + " (phenomenon_time >= " + str(timeranges[i][0]) + " AND phenomenon_time <= " + str(timeranges[i][1]) + ")"
    timeranges_string = timeranges_string + ")"
    query = """
    SELECT
        phenomenon_time,result,datastream_id
    FROM
        observation_dbs
    WHERE
        datastream_id IN """ + ids_string + """
        AND """ + timeranges_string + """
    ORDER BY
        phenomenon_time ASC
    LIMIT """ + str(limit)
    
    return query

def get_relevant_observations(datastream_ids: list, limit: int):
    """
    Returns a query that returns all observations (until the limit is reached) for the given datastream IDs.
    """
    
    ids_string = "("
    for datastream_id in datastream_ids:
        ids_string = ids_string + str(datastream_id) + ","
    ids_string = ids_string[:-1]
    ids_string = ids_string + ")"
    
    query = """
    SELECT
        phenomenon_time,result,datastream_id
    FROM
        observation_dbs
    WHERE
        datastream_id IN """ + ids_string + """ AND phenomenon_time >= 1694649600
    ORDER BY
        phenomenon_time ASC
    LIMIT """ + str(limit)
    
    return query