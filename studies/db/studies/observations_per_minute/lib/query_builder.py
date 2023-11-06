def get_observations_per_minute_query(datastream_ids: list, only_mqtt: bool):
    if only_mqtt:
        filter = " AND mqtt = true"
    else:
        filter = ""

    ids_string = "("
    for datastream_id in datastream_ids:
        ids_string = ids_string + str(datastream_id) + ","
    ids_string = ids_string[:-1]
    ids_string = ids_string + ")"

    # Query:
    # 1. Filter by datastream IDs
    # 2. Divide every phenomenon_time by 60 and round down
    # 3. Aggregate by rounded phenomenon_time and count
    # 4. Return rounded phenomenon_time and count
    # 5. Order by rounded phenomenon_time
    query = """
    SELECT
        COUNT(*) as count,
        FLOOR(phenomenon_time/60) as rounded_phenomenon_time
    FROM
        observation_dbs
    WHERE
        datastream_id IN """ + ids_string + filter + """
    GROUP BY
        rounded_phenomenon_time
    ORDER BY
        rounded_phenomenon_time
    """

    return query


def get_observations_per_hour_query(datastream_ids: list):
    ids_string = "("
    for datastream_id in datastream_ids:
        ids_string = ids_string + str(datastream_id) + ","
    ids_string = ids_string[:-1]
    ids_string = ids_string + ")"

    # Query:
    # 1. Filter by datastream IDs
    # 2. Divide every phenomenon_time by 60 and round down
    # 3. Aggregate by rounded phenomenon_time and count
    # 4. Return rounded phenomenon_time and count
    # 5. Order by rounded phenomenon_time
    query = """
    SELECT
        COUNT(*) as count,
        FLOOR(phenomenon_time/3600) as rounded_phenomenon_time
    FROM
        observation_dbs
    WHERE
        datastream_id IN """ + ids_string + """
    GROUP BY
        rounded_phenomenon_time
    ORDER BY
        rounded_phenomenon_time
    """

    return query
