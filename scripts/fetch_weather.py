from datetime import date, timedelta

import pandas as pd
import requests


def main():
    base = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Seattle/'
    key = 'LE84LV2Y4E6ADSWQUMHQS3JUH'

    d = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    url = base + f"{d}/{d}?unitGroup=metric&key={key}&contentType=json&include=hours"

    result = requests.get(url).json()

    entries = []
    for day in result['days']:
        for hour in day['hours']:
            del hour['stations'], hour['source']
            hour['day'] = day['datetime']
            entries.append(hour)

    df = pd.DataFrame.from_records(entries)
    cols = df.columns.tolist()
    cols[1], cols[-1] = cols[-1], cols[1]

    df = df[["datetime", "day", "temp", "humidity", "windgust", "pressure", "visibility"]]
    df.to_json(f'./data/weather/Seattle-weather.json', orient='records')


if __name__ == '__main__':
    main()