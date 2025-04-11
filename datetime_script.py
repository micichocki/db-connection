from datetime import datetime, timedelta

def generate_timestamp(hour: int = 1, days_offset: int = 0, timestamp = False) -> str:
    """
    Generuje timestamp na podstawie godziny (0-23) i liczby dni od 2020-01-01.

    :param hour: Godzina (0-23)
    :param days_offset: Liczba dni od 2020-01-01 (liczba naturalna)
    """
    if not (0 <= hour <= 23):
        raise ValueError("Godzina musi być w zakresie 0-23")
    if days_offset < 0:
        raise ValueError("Liczba dni musi być liczbą naturalną (>=0)")

    start_date = datetime(2020, 1, 1)
    result_date = start_date + timedelta(days=days_offset)
    result_date = result_date.replace(hour=hour, minute=0, second=0)
    
    if (timestamp):
        return int(result_date.timestamp())
    
    return result_date.strftime("%Y-%m-%d %H:%M:%S")