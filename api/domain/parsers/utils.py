def normalize_stream_data(data):
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        return data.values()

    return []